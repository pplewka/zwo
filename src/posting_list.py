import sqlite3
from math import log
from sys import maxsize as lastID
from dataclasses import dataclass, field
from sortedcontainers import SortedList
from typing import List, Iterable, Dict

DBConnection = sqlite3.Connection


@dataclass(frozen=True)
class Posting:
    """Definition of a single posting"""
    did: int
    tf: int = field(compare=False)

    def __repr__(self):
        return f'{self.did}|{self.tf}'

    def __eq__(self, value):
        return self.did == value.did

    def __lt__(self, value):
        return self.did < value
    
    def __gt__(self, value):
        return self.did > value
    
    def __le__(self, value):
        return self.did <= value

    def __ge__(self, value):
        return self.did >= value

class PostingList:
    term: str
    data: SortedList
    iterid: int
    df: int

    def __init__(self, df: int, term: str, data: Iterable):
        self.term = term 
        self.df = df
        self.data = SortedList(data)
        self.iterid = 0

    def next(self, p = -1):
        if p == -1:
            """Sequential iteration"""
            self.iterid+=1
            return self.data[self.iterid]
        else:
            """Skip iteration"""
            self.iterid = self.data.bisect_left(p)
            if self.iterid == len(self.data):
                # If we get a "new" index, we know that no such document existed and we have to return the special lastID Posting
                return Posting(lastID, 0)
            else:
                return self.data[self.iterid]

    def posting(self) -> Posting:
        """Return the posting that the iterator currently points to."""
        return self.data[self.iterid]

    def __repr__(self):
        return f'{self.term} is at {self.data[self.iterid]}'
    
        

class TermList:
    index_name_dict: Dict[str, int]
    postings: List[PostingList]

    def __init__(self):
        self.index_name_dict = {}
        self.postings = []
    
    def add_pl(self, pl: PostingList):
        self.postings.append(pl)
        self.index_name_dict[pl.term] = len(self.postings) - 1
    
    def sort(self, curr_postings):
        """Sort the term posting in increasing order or the DID's of their current posting."""
        # sorted() call to sort the current postings by their id.
        # Then walk through the term list, finding the posting list that belongs to the first element, then the posting list that belongs to the second element etc. 
        # @TODO This can be improved.
        self.postings = [pl for x in sorted(curr_postings, key=lambda e: e[1]) for pl in self.postings if pl.term == x[0]]
        # Rebuild the list index <-> term dict
        for i in range(len(self.postings)):
            self.index_name_dict[self.postings[i].term] = i
    
    def get(self, term: str) -> PostingList:
        """Get the posting list for a given term"""
        return self.postings[self.index_name_dict[term]]
    
    def terms_as_list(self):
        return [pl.term for pl in self.postings]


class InvertedIndex:
    connection: DBConnection

    def __init__(self, connection: DBConnection):
        self.connection = connection

    def getIndexList(self, term: str) -> PostingList:
        return PostingList(term=term, df=self.getDF(term), data=[Posting(did, tf) for did, tf in self.connection.execute("SELECT did, tf FROM tfs where term = ?", (term,))])

    def getDF(self, term: str) -> int:
        """Return the document frequency for a given term."""
        r = self.connection.execute("""
            SELECT df from dfs WHERE term = ?
        """, (term,))
        return int(r.fetchone()[0])

    def getSize(self) -> int:
        """Return the size of the document collection"""
        r = self.connection.execute("""
            SELECT size from d
        """)
        return int(r.fetchone()[0])

    def getLength(self, did: int) -> int:
        """Return the length (number of term occurances) for a document identifier."""
        r = self.connection.execute("""
            SELECT len from dls WHERE did=:did     
        """, (did,))
        return int(r.fetchone()[0])
    
    def getUpperBound(self, term: str) -> float:
        """Return the upper bound for the score for a given term."""
        r = self.connection.execute("""
            SELECT max FROM ub WHERE term=:term
        """, (term,))
        idf = log(self.getSize() / self.getDF(term))
        tf = int(r.fetchone()[0])

        return tf * idf

        
def create_indices(connection: DBConnection):
    print("\n[-] creating index tfs_idx", end="")
    connection.execute("""
        CREATE INDEX tfs_idx ON tfs(term, did)
    """)
    print("\r[+] creating table tfs_idx")
    print("\n[-] creating index docs_idx", end="")
    connection.execute("""
        CREATE INDEX docs_idx ON docs(did)
    """)
    print("\r[+] creating table docs_idx")
    print("\n[-] creating index dfs_idx", end="")
    connection.execute("""
        CREATE INDEX dfs_idx ON dfs(term, df)
    """)
    print("\r[+] creating table dfs_idx")
    print("\n[-] creating index dls_idx", end="")
    connection.execute("""
        CREATE INDEX dls_idx ON dls(did, len)
    """)
    print("\r[+] creating table dls_idx")
    print("\n[-] creating index ub_idx", end="")
    connection.execute("""
        CREATE INDEX ub_idx ON ub(term)
    """)
    print("\r[+] creating table ub_idx")