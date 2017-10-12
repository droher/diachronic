from subprocess import Popen, PIPE
from lxml import etree
from lxml.etree import Element
from typing import Tuple
import gzip
import bsdiff4

from diachronic.conf import DEFAULT_PATH

WIKI_PATH = DEFAULT_PATH + "enwiki-20170901-pages-meta-history1.xml-p10p2123.7z"
OUTPUT_PATH = DEFAULT_PATH + "test.7z"
WIKI_NS = "{http://www.mediawiki.org/xml/export-0.10/}"
REVISION_POWER = 1.11
MAX_REVISION = 1E9
BUFF_SIZE = 1E8


def ltag(elem: Element) -> str:
    return etree.QName(elem).localname


def nstag(tag: str) -> str:
    return WIKI_NS + tag


def wiki_gen(stream) -> Tuple[str, Element]:
    for event, elem in etree.iterparse(stream.stdout, events=("end", ), tag=WIKI_NS+"page"):
        yield event, elem


def wiki_stream(path: str) -> Popen:
    return Popen(["7z", "e", "-so", path], stdout=PIPE)


def make_valid_indexes(rev_power: float=REVISION_POWER, max_index: int=MAX_REVISION):
    s = {0}
    index = 0
    for i in range(int(max_index)):
        new_index = int(min(max_index - 1, max(index + 1, rev_power ** i)))
        if new_index <= index:
            break
        s.add(new_index)
        index = new_index
    return s


def run():
    buff = bytes()
    count = 0
    valid_indexes = make_valid_indexes()
    with gzip.open(OUTPUT_PATH, 'wb') as f:
        for event, elem in wiki_gen(wiki_stream(WIKI_PATH)):
            count+=1
            if len(buff) > BUFF_SIZE:
                print("Writing buffer...")
                f.write(buff)
                print("Buffer flushed")
                buff = ""
            title = elem.find(nstag("title")).text
            print(count, title, len(buff), len(buff)/BUFF_SIZE)
            revisions = ((child.find(nstag("timestamp")).text, child.find(nstag("text")).text)
                         for i, child in enumerate(elem.iter(nstag("revision")))
                         if i in valid_indexes)
            past = bytes(next(revisions)[1] or "", 'UTF-8')
            buff += past
            for tstamp, text in revisions:
                cur = bytes(text or "", 'UTF-8')
                if past and cur:
                    diffs = bsdiff4.diff(past, cur)
                    buff += diffs
                past = cur
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        print("Final write")
        f.write(buff)


if __name__ == "__main__":
    run()
