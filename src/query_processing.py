import sqlite3
import heapq
from math import log
from typing import List
from dataclasses import dataclass, field
from parser import Parser
from posting_list import InvertedIndex

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

    def process(self, query: str, k: int = -1) -> List[Accumulator]:
        """Process a query string and return the weighted results.
            @param: query - the query string
            @param: k - number of top k results to return, if empty, default of -1 is used, indicating all results.  
        """
        terms = Parser.tokenize([query])
        results = dict()

        # print(f'Processing terms: {terms}')

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
