import sqlite3
import heapq
from dataclasses import dataclass, field
from typing import List

DBConnection = sqlite3.Connection


@dataclass(order=True, frozen=True)
class Posting:
    """Definition of a single posting"""
    did: int
    tf: int = field(compare=False)
    page: int = field(compare=False)
    date: int = field(compare=False)

    def __repr__(self):
        return f'{self.did}|{self.tf}'


class InvertedIndex:
    connection: DBConnection

    def __init__(self, connection: DBConnection):
        self.connection = connection

    def getIndexList(self, term: str) -> List[Posting]:
        h = []
        for did, tf in self.connection.execute("SELECT did, tf FROM tfs where term = ?", (term,)):
            heapq.heappush(h, Posting(did, tf, self.getPage(did), self.getDate(did)))

        return [heapq.heappop(h) for _ in range(len(h))]

    def getDF(self, term: str) -> int:
        """Return the document frequency for a given term."""
        r = self.connection.execute("""
            SELECT df from dfs WHERE term = ?
        """, (term,))
        return int(r.fetchone()[0])

    def getPage(self, did: int) -> int:
        """Return the page of a document"""
        return self.connection.execute("SELECT page FROM boost WHERE did = ?", (did,)).fetchone()[0]

    def getDate(self, did: int) -> int:
        """Return the date of a document"""
        return self.connection.execute("SELECT date FROM boost WHERE did = ?", (did,)).fetchone()[0]

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


def create_indices(connection: DBConnection):
    print("\n[-] creating index tfs_idx", end="")
    connection.execute("""
        CREATE INDEX tfs_idx ON tfs(term, did)
    """)
    print("\r[+] creating index tfs_idx")
    print("\n[-] creating index docs_idx", end="")
    connection.execute("""
        CREATE INDEX docs_idx ON docs(did)
    """)
    print("\r[+] creating index docs_idx")
    print("\n[-] creating index dfs_idx", end="")
    connection.execute("""
        CREATE INDEX dfs_idx ON dfs(term, df)
    """)
    print("\r[+] creating index dfs_idx")
    print("\n[-] creating index dls_idx", end="")
    connection.execute("""
        CREATE INDEX dls_idx ON dls(did, len)
    """)
    print("\r[+] creating index dls_idx")
    print("\n[-] creating index boost_idx", end="")
    connection.execute("""
            CREATE INDEX boost_idx ON boost(did, date, page)
        """)
    print("\r[+] creating index boost_idx")
