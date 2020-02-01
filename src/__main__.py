import os
import time
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
    print(r'''
                                                          .-.   .-.                                .-.  .-.
                                                          |  \ /  |                                |  /\  |     
                                                          /,   ,_  `'-.                        .-'`  _ˎ   ˎ\            
                                                        .-|\   /`\     '.                    .'     /`\   /|-.          
                                                      .'  0/   | 0\  \_  `".              ."`  _/  /0 |   |0  '.     
                                                   .-'  _,/    '--'.'|#''---'            '---''#|'.'--'    \ˎ_  '-.____________________________________ 
                                                   `--'  |       /   \#                      #/:  \       |  '--`  |                                   |
                                                         |      /     \#                    #/.    \      |        |          HAPPY SEARCHING          |   
                                                         \     ;|\    .\#                  #/    ./|;     /       /                                    |
                                                         |' ' //  \   ::\#                #/   ::/   \ ' '|      /_____________________________________|
                                                         \   /`    \   ':\#              #/   ':/    `\   /  
                                                          `"`       \..   \#            #/   ../       `"`            
                                                        /|           \::.  \#          #/.   ./
 ______________________________________________________/ |            \::   \#        #/::   /
|                                                        |             \'  .:\#      #/:::: /     
|                                                        |              \  :::\#    #/:::  /     
|                   WELCOME TO ZWO                       |               \  '::\#  #/::'  / 
|  - A Search Engine for the New York Times Corpus 2000  |                \     \##/     /
|                                                        |                 \            /
|________________________________________________________|                  |          |''')

    if os.path.isfile("nyt.sqlite"):

        with open_db() as connection:
            query = input("\nSearch: ")
            processor = QueryProcessor(connection)

            time_stamp = time.time()
            accumulators = processor.process(query, k=10)
            elapsed_time = time.time() - time_stamp
            print(f'\nHere are the Top-10 results for {query}')
            print(f'Found in {round(elapsed_time, 2)} seconds.\n')
            for i, acc in enumerate(accumulators):
                print(f'{i+1}.\tscore: {int(acc.score)}\turl: {get_url(connection, acc.did)}\ttitle: {get_headline(connection, acc.did)}')

    else:
        create_db()
        dir = input("Please tell me the path to the diretory of the nyt corpus: ")
        parse_dir(dir)
        with open_db() as connection:
            create_indices(connection)
