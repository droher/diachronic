from subprocess import Popen, PIPE
from lxml import etree
from lxml.etree import Element
from typing import Tuple
import gzip
import bsdiff4

from diachronic.conf import DEFAULT_PATH

WIKI_PATH = DEFAULT_PATH + "enwiki-20170901-pages-meta-history27.xml-p42663462p42922763.7z"
OUTPUT_PATH = DEFAULT_PATH + "bigtest.gzip"
WIKI_NS = "{http://www.mediawiki.org/xml/export-0.10/}"
REVISION_POWER = 1.1
MAX_REVISION = 1E9
BUFF_SIZE = 1E8


def ltag(elem: Element) -> str:
    return etree.QName(elem).localname


def nstag(tag: str) -> str:
    return WIKI_NS + tag


def wiki_gen(stream) -> Tuple[str, Element]:
    for event, elem in etree.iterparse(stream.stdout):
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
    global_buffer = bytes()
    local_buffer = bytes()
    count = 0
    valid_indexes = make_valid_indexes()
    current_revision = None
    rev_index = 0
    revs_parsed = 0
    with gzip.open(OUTPUT_PATH, 'wb') as f:
        for event, elem in wiki_gen(wiki_stream(WIKI_PATH)):
            tag = ltag(elem)
            if tag == "revision":
                if rev_index in valid_indexes:
                    text = bytes(elem.find(nstag("text")).text or "", 'UTF-8')
                    if rev_index == 0:
                        current_revision = text
                        local_buffer += text
                    else:
                        local_buffer += bsdiff4.diff(current_revision, text)
                        current_revision = text
                    revs_parsed += 1
                rev_index += 1
            elif tag == "title":
                title = elem.text
            elif tag == "page":
                global_buffer += bytes(title, 'UTF-8') + local_buffer
                print(count, title, rev_index + 1, revs_parsed, len(global_buffer)/BUFF_SIZE)

                count += 1
                current_revision = None
                rev_index = 0
                revs_parsed = 0
                local_buffer = bytes()

                if len(global_buffer) > BUFF_SIZE:
                    print("Writing buffer...")
                    f.write(global_buffer)
                    print("Buffer flushed")
                    global_buffer = bytes()

                while elem.getprevious() is not None:
                    del elem.getparent()[0]

            elem.clear()

        print("Final write")
        f.write(global_buffer)


if __name__ == "__main__":
    run()
