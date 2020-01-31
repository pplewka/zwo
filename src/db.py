import sqlite3
from parser import Document
from typing import Sequence, Callable, TypeVar

DB_NAME = "nyt.sqlite"
STATEMENT_CACHE = 100000
STATS_FUNCS = dict()
DBConnection = sqlite3.Connection
T = TypeVar('T')


def chunks(seq: Sequence[T], n: int = 1000) -> Sequence[Sequence[T]]:
    # we batch 1000 sql commands instead of 10. 10 was extremely slow
    """Divide an iterable into chunks of size n"""
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def compute_statistics(connection: DBConnection) -> None:
    """Compute the dfs, dsl, d statistics and write them to the db"""
    for f in STATS_FUNCS.values():
        f(connection)


def create_db(db_name: str = DB_NAME) -> DBConnection:
    """Creates a new database with given name. Only the empty tables docs and tfs will be present after this."""
    connection = open_db(db_name)
    connection.execute("""
        CREATE TABLE docs
        (did INTEGER PRIMARY KEY, 
        title TEXT NOT NULL, 
        url TEXT NOT NULL)
    """)
    connection.execute("""
        CREATE TABLE tfs 
        (did INTEGER,
        term TEXT NOT NULL,
        tf INTEGER)
    """)
    connection.execute("""
        CREATE TABLE boost
        (did INTEGER,
        date INTEGER,
        page INTEGER
        )""")
    print(f"[+] Created db {DB_NAME}")
    return connection


def open_db(db_name: str = DB_NAME) -> DBConnection:
    """Opens the database with given name"""
    return sqlite3.connect(db_name, cached_statements=STATEMENT_CACHE)


def insert_documents(connection: DBConnection, documents: Sequence[Document]) -> None:
    """Inserts all documents into the docs table"""
    max_ = len(documents)
    current = 0
    print()  # print an extra line, because we will delete lines with printing \r
    for chunk in chunks(documents):
        connection.execute("BEGIN TRANSACTION")
        for doc in chunk:
            # python doesn't support prepared statements, but instead has a builtin sql cache
            connection.execute(
                "INSERT INTO docs(did, title, url) VALUES (?, ?, ?)", doc.convert_to_tuple())
            current += 1
            print(f"\r[{current}/{max_}] doc done", end='')
        connection.execute("COMMIT")


def insert_boost(connection: DBConnection, documents: Sequence[Document]) -> None:
    """Inserts all values into the boost table"""
    max_ = len(documents)
    current = 0
    print()  # print an extra line, because we will delete lines with printing \r
    for chunk in chunks(documents):

        connection.execute("BEGIN TRANSACTION")
        for doc in chunk:
            connection.execute(
                "INSERT INTO boost(did, date, page) VALUES (?, ?, ?)", (doc.id, doc.date, doc.page))
        connection.execute("COMMIT")
        current += len(chunk)
        print(f"\r[{current}/{max_}] boost done", end='')
    print()


def insert_tfs(connection: DBConnection, documents: Sequence[Document]) -> None:
    """Inserts all term frequencies into the tfs table"""
    max_ = len(documents)
    current = 0
    print()  # print an extra line, because we will delete lines with printing \r
    for chunk in chunks(documents):
        rows = (d.get_tfs_rows() for d in chunk)
        connection.execute("BEGIN TRANSACTION")
        for row in rows:
            connection.executemany(
                "INSERT INTO tfs(did, term, tf) VALUES (?, ?, ?)", row)
        connection.execute("COMMIT")
        current += len(chunk)
        print(f"\r[{current}/{max_}] doc-tfs done", end='')
    print()


def get_headline(connection: DBConnection, did: int):
    """Retrieves the headline of a article in the db"""
    return connection.execute("SELECT title FROM docs WHERE did=:did", (did,)).fetchone()[0]


def get_max_page(connection: DBConnection) -> int:
    """Retrieves the maximum page of a article in the db"""
    return connection.execute("SELECT max_page FROM max_page").fetchone()[0]


def collection_statistic(func: Callable) -> Callable:
    """Decorator Function to mark a function as computing statistics."""
    STATS_FUNCS[func.__name__] = func
    return func


@collection_statistic
def create_and_insert_dls(connection: DBConnection) -> None:
    """Creates and fills the table dls with document ids and the length of the document"""
    print("\n[-] creating table dls", end="")
    connection.execute("""
        CREATE TABLE dls AS
        SELECT did, SUM(tf) AS len FROM tfs GROUP BY did
    """)
    print("\r[+] creating table dls")


@collection_statistic
def create_and_insert_dfs(connection: DBConnection) -> None:
    """Creates and fills the table dfs with terms and their document frequencies"""
    print("\n[-] creating table dfs", end="")
    connection.execute("""
        CREATE TABLE dfs AS
        SELECT term, COUNT(tf) AS df FROM tfs GROUP BY term
    """)
    print("\r[+] creating table dfs")


@collection_statistic
def create_and_insert_d(connection: DBConnection) -> None:
    """Create and fills the table d with the total number of documents in the collection"""
    print("\n[-] creating table d", end="")
    connection.execute("""
        CREATE TABLE d AS
        SELECT COUNT(DISTINCT did) AS size FROM tfs
    """)
    print("\r[+] creating table d")


@collection_statistic
def create_and_insert_max_page(connection: DBConnection) -> None:
    """Create and fills the table max_page with the maximum page number in the collection"""
    print("\n[-] creating table max_page", end="")
    connection.execute("""
    CREATE TABLE max_page AS
    SELECT MAX(page) AS max_page from boost""")
    print("\r[+] creating table max_page")
