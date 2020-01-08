import os
from pathlib import Path
from typing import List
from parser import Parser, Document


class Importer:
    """Importer class used for finding all xml files in a given directory"""

    @staticmethod
    def import_dir(path: str) -> List[Document]:
        """
        Searches for all xml files in a directory and all its subdirectories,
        prints them with their size and returns a list with their paths
        :param path: a directory
        :return: a list with all paths of xml files
        """
        results: List[Document] = []
        for dirname, _, files in os.walk(path):
            for f in files:
                p = Path(dirname).joinpath(f)
                if p.suffix == ".xml":
                    print(f"File: {p} with size {p.stat().st_size} bytes")
                    results.append(Parser.parse(p))  # we don't insert them right now, we insert them in chunks later
        return results
