import sqlite3
import heapq
from math import log
from sys import maxsize as lastID
from typing import List, Dict, Iterable
from dataclasses import dataclass, field
from parser import Parser
from posting_list import InvertedIndex, Posting, PostingList, TermList

DBConnection = sqlite3.Connection


@dataclass(order=True)
class Accumulator:
    did: int = field(compare=False)
    score: float

    def __eq__(self, other):
        return self.did == other.did

    def __add__(self, other):  # Overload "+" Operator
        if self == other:
            return Accumulator(self.did, self.score + other.score)
        else:
            raise ValueError

    def __iadd__(self, other):  # Overload "+=" assignment
        if isinstance(other, Accumulator):
            self.score += other.score
            return self
        else:
            self.score += other
            return self


class QueryProcessor:

    def __init__(self, connection: DBConnection):
        self.index = InvertedIndex(connection)
        self.collection_size = self.index.getSize()

    def process_with_wand(self, query: str, k: int):
        terms = Parser.tokenize([query])

    def process(self, query: str, k: int = -1) -> List[Accumulator]:
        """Process a query string and return the weighted results.
            @param: query - the query string
            @param: k - number of top k results to return, if empty, default of -1 is used, indicating all results.  
        """
        terms = Parser.tokenize([query])
        results = dict()

        print(f'Processing terms: {terms}')

        for t in terms:
            try:
                df = self.index.getDF(t)
            except TypeError:
                continue
            plist = self.index.getIndexList(t)
            term_specific_constant = log(self.collection_size / df)
            for posting in plist:
                acc = self.score(posting.did, posting.tf, term_specific_constant)
                try:  # Try to sum up the values
                    results[posting.did] += acc
                except KeyError:  # No posting for this did has been seen yet.
                    results[posting.did] = acc

        if k == -1:
            return sorted(results.values(), reverse=True)
        return heapq.nlargest(k, results.values())

    @staticmethod
    def score(did: int, tf: int, term_specific_constant: float):
        return Accumulator(did=did, score=tf * term_specific_constant)

class WANDIterator:
    index: InvertedIndex
    posting: Dict[str, Posting]
    terms: TermList
    curDoc: int


    def __init__(self, query_terms: Iterable[str], index: InvertedIndex):
        self.posting = dict()
        self.terms = TermList()
        self.index = index
        self.curDoc = 0
        for term in query_terms:
            self.terms.add_pl(self.index.getIndexList(term))
            self.posting[term] = self.terms.get(term).posting()


    def wand_next(self, theta):
        while True:
            # Sort the terms in non decreasing order of DID
            self.terms.sort(self.posting.items())
            sorted_terms = self.terms.terms_as_list()

            # Find pivot term - the first one with accumulated UB >= theta
            pivot_term = self.find_pivot_term(sorted_terms, theta)
            if pivot_term is None: return None # No more docs. 
            pivot = self.posting[pivot_term].did
            if pivot == lastID: return None # No more docs.

            if pivot <= self.curDoc:
                # Pivot has already been considered, we need to advance one of the preceding terms.
                aterm = self.pick_term(sorted_terms[0:sorted_terms.index(pivot_term)])
                self.posting[aterm] = self.terms.get(aterm).next(pivot)
            else:
                if all([pivot == self.posting[t].did for t in sorted_terms[0:sorted_terms.index(pivot_term)]]):
                    self.curDoc = pivot
                    return (self.curDoc, self.posting)
                else:
                    # Not enough mass on pivot, we need to advance one of the preceding terms
                    aterm = self.pick_term(sorted_terms[0:sorted_terms.index(pivot_term)])
                    self.posting[aterm] = self.terms.get(aterm).next(pivot)


    def find_pivot_term(self, terms: Iterable[str], threshold):
        """find_pivot_term returns the first term in the given order for which the accumulated upper bounds of all terms preceding it and including it, exceed a threshold."""
        acc = 0
        for term in terms:
            acc += self.index.getUpperBound(term)
            if acc > threshold: return term
        # If we can't find it, we must be done
        return None
    

    def pick_term(self, terms: Iterable[str]):
        """pick_term selects a term whose iterator should be advanced. This is done by picking the term with the smalled df (which is the same as the term with the largest idf)"""
        min = (None, lastID)
        for t in terms:
            df = self.terms.get(t).df
            if df < min[1]: min = (t,df)
        
        return min[0]


if __name__ == "__main__":
    from db import *
    with open_db() as connection:
        index = InvertedIndex(connection)
        wand = WANDIterator(query_terms=["tokyo", "train", "disaster"], index=index)
        res = wand.wand_next(0)
        print("end")
