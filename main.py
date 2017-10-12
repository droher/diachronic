from subprocess import Popen, PIPE
from lxml import etree
from lxml.etree import Element
from typing import Tuple
import gzipg
from sys import getsizeof

from diachronic.conf import DEFAULT_PATH
from diachronic.diff_match_patch import diff_match_patch

WIKI_PATH = DEFAULT_PATH + "enwiki-20170901-pages-meta-history1.xml-p10p2123.7z"
OUTPUT_PATH = DEFAULT_PATH + "test.7z"
WIKI_NS = "{http://www.mediawiki.org/xml/export-0.10/}"
REVISION_POWER = 1.11
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


def run():
    buff = ""
    dmp = diff_match_patch()
    count = 0
    with gzip.open(OUTPUT_PATH, 'wb') as f:
        for event, elem in wiki_gen(wiki_stream(WIKI_PATH)):
            count+=1
            if len(buff) > BUFF_SIZE:
                print("Writing buffer...")
                f.write(bytes(buff, 'UTF-8'))
                print("Buffer flushed")
                buff = ""
            title = elem.find(nstag("title")).text
            print(count, title, len(buff), len(buff)/BUFF_SIZE)
            buff += title + "\n"
            revisions = ((child.find(nstag("timestamp")).text, child.find(nstag("text")))
                         for child in elem.iter(nstag("revision")))
            revisions = sorted(revisions, key=lambda x: x[0])
            t1 = revisions[0][1].text
            t2 = revisions[1][1].text
            buff += t1 + "\n"
            index = 1
            for i in range(1, len(revisions)):
                if t1 and t2:
                    diffs = dmp.diff_main(t1, t2, checklines=True)
                    buff += "{}]\n".format(diffs)
                new_index = min(len(revisions) - 1, max(index + 1, int(REVISION_POWER ** i)))
                if index == new_index:
                    break
                t1 = revisions[index][1].text
                t2 = revisions[new_index][1].text
                index = new_index
            elem.clear()
            del revisions
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        print("Final write")
        f.write(bytes(buff, 'UTF-8'))


if __name__ == "__main__":
    run()
