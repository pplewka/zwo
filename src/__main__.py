import pprint
import argparse
import time
from pathlib import Path
from parser import Parser
from posting_list import InvertedIndex, create_indices
from query_processing import QueryProcessor
from importer import Importer
from db import *


def parse_dir(directory: str) -> None:
    """Takes a directory path and parses all Documents inside this path, writing the results to a file."""
    docs = Importer.import_dir(directory)
    with open_db() as connection:
        insert_documents(connection, docs)
        insert_tfs(connection, docs)
        insert_boost(connection, docs)
        compute_statistics(connection)


if __name__ == "__main__":
    argparser = argparse.ArgumentParser(
        description="Parse and tokenize the 2000 NYT Corpus.")
    options = argparser.add_mutually_exclusive_group()
    options.add_argument("-p", "--parser", action="store_true",
                         help="Parse a single document and print it stdout")
    options.add_argument("-i", "--importer", action="store_true",
                         help="Print all the document files in a directory path")
    options.add_argument("-r", "--recursive", action="store_true",
                         help="Recursively walk a directory, parsing all documents inside and add them to the database."
                              " (default)")
    options.add_argument("-q", "--query_processing", action="store_true",
                         help="Search for a query in the database and print all findings")
    options.add_argument("-k", "--top-k-results", nargs=1, type=int, default=-1, metavar="k",
                         help="Search for a query in the database and print the top k findings")
    argparser.add_argument("path_or_query", help="file or directory path or search query depending on other parameters",
                           nargs='+')
    args = argparser.parse_args()
    args.path_or_query = ' '.join(args.path_or_query)
    if args.parser:
        document = Parser.parse(Path(args.path_or_query))
        pprint.pprint(document)
    elif args.importer:
        Importer.import_dir(args.path_or_query)
    elif args.query_processing or args.top_k_results != -1:
        k = -1 if args.query_processing else args.top_k_results[0]
        with open_db() as connection:
            processor = QueryProcessor(connection)
            #  for query in ("olympics opening ceremony", "denmark sweden bridge", "tokyo train disaster"):
            time_stamp = time.time()
            accumulators = processor.process(args.path_or_query, k=k)
            elapsed_time = time.time() - time_stamp
            print(f'Found in {round(elapsed_time, 2)} seconds.')
            for i, acc in enumerate(accumulators):
                print(f' {i}. score={int(acc.score)} did={acc.did} {get_headline(connection, acc.did)}')
    else:
        create_db()
        parse_dir(args.path_or_query)
        with open_db() as connection:
            create_indices(connection)
