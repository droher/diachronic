from subprocess import Popen, PIPE
from lxml import etree
from lxml.etree import Element
from typing import Tuple

from .conf import DEFAULT_PATH

WIKI_PATH = DEFAULT_PATH + "enwiki-20170901-pages-meta-history1.xml-p10p2123.7z"
WIKI_NS = "{http://www.mediawiki.org/xml/export-0.10/}"


def ltag(elem: Element) -> str:
    return etree.QName(elem).localname


def nstag(tag: str) -> str:
    return WIKI_NS + tag


def wiki_gen(stream) -> Tuple[str, Element]:
    for event, elem in etree.iterparse(stream.stdout, events=("start", "end")):
        yield event, elem


def wiki_stream(path: str) -> Popen:
    return Popen(["7z", "e", "-so", path], stdout=PIPE)


def main():
    count = 0
    for event, elem in wiki_gen(wiki_stream(WIKI_PATH)):
        count += 1
        if count % 10000 == 0:
            print(count)
        if event == "end" and ltag(elem) == "page":
            title = elem.find(nstag("title")).text
            revisions = [(child.find(nstag("timestamp")).text, child.find(nstag("text")))
                         for child in elem.iter(nstag("revision"))]
            revisions = sorted(revisions, key=lambda x: x[0])
            elem.clear()
            del revisions
            while elem.getprevious() is not None:
                del elem.getparent()[0]


if __name__ == "__main__":
    main()
