import re
import xml.etree.ElementTree as XML
from collections import Counter
from dataclasses import dataclass
from typing import Sequence, Union, Iterable, Tuple, List
from pathlib import Path
import constants


@dataclass
class Document:
    """Definition of a document"""
    id: int
    title: str
    url: str
    content: Sequence[str]
    content_counter: Counter
    title_counter: Counter

    def __init__(self, id: int, title: str, url: str, content: Sequence[str]):
        self.id = id
        self.title = title
        self.url = url
        self.content = content
        # count term frequencies on initialization, saving us a lot of time later.
        self.content_counter = Counter(self.content)
        self.title_counter = Counter(Parser.tokenize([self.title]))

    def __repr__(self):
        from pprint import pformat
        return pformat(vars(self), indent=2, compact=True)

    def convert_to_tuple(self) -> Tuple:
        """Converts the document into a tuple, ready to be inserted into the docs table"""
        return self.id, self.title, self.url

    def get_tfs_rows(self) -> Iterable:
        """Returns all rows for the tfs table of this document"""
        for term in self.content_counter.keys():
            yield (self.id, term, self.content_counter[term] * constants.TUNABLE_WEIGHT_CONTENT +
                   self.title_counter[term] * constants.TUNABLE_WEIGHT_TITLE)


class Parser:
    """Parser class taking an XML file and turning it into a Document object"""

    # The regex consists of two parts. With [^a-zA-Z0-9 ]+ we match all the characters that we want to remove,
    # such as full stops, non - alphanumeric characters etc. Next, we use a negative lookbehind to find acronyms and
    # exclude them from the matches. An acronym as consisting at least two upper or lowercase characters, each followed
    # by a dot, followed by a whitespace.
    __TOKENIZE_REGEX = r'[^a-zA-Z0-9 ]+(?<!( |\.)[a-zA-Z]\.)'
    __COMPILED_REGEX = re.compile(__TOKENIZE_REGEX)

    @staticmethod
    def parse(path: Path) -> Document:
        """Takes a XML Document and parses it to a Document. Performs tokenization for the content as well"""
        with path.open('r') as input_:
            prospective_doc = _nytcorpus_to_document(XML.parse(input_))

        content = Parser.tokenize(prospective_doc[3])
        return Document(prospective_doc[0], prospective_doc[1], prospective_doc[2], content)

    @staticmethod
    def tokenize(content: List[str]) -> List[str]:
        # Begin tokenizing

        # Replace non alphanumeric while keeping abbreviations with whitespace.
        # Uses a "negative" regex, matching only the things that need to be removed,
        # instead of finding the things to keep.
        cleaned = (re.sub(Parser.__COMPILED_REGEX, " ", s) for s in content)
        # Lowercase and split at whitespace.
        return ' '.join([par.lower() for par in cleaned]).split()


def _nytcorpus_to_document(root: Union[XML.Element, XML.ElementTree]) -> Tuple[int, str, str, List[str]]:
    """ Simple XML parsing function that extracts our Document object from a given news article.

        The content field of the returned document will not be tokenized.
        They are still a Sequence of strings, each string representing a new paragraph.
    """
    from sys import stderr
    head = root.find("./head")
    body = root.find("./body")
    docdata = head.find("./docdata")
    pubdata = head.find("./pubdata")
    id_ = "-1"  # fallback value
    try:
        id_ = docdata.find("./doc-id").get('id-string')
        title = head.find("./title")
        if title is None:
            print("Document {} had no title.".format(id_), file=stderr)
            title = "NO TITLE FOUND"
        else:
            title = title.text

        url = pubdata.get('ex-ref')
        # Already cleans out all the HTML Elements
        content = [par.text for par in body.findall(
            "./body.content/*[@class='full_text']/p")]
    except AttributeError as attr:
        # We can't do much if finding a url or the content fails.
        print("Attribute error for document ID: " + id_, file=stderr)
        print(attr, file=stderr)
        return int(id_), "Error", "Error", []

    return int(id_), title, url, content
