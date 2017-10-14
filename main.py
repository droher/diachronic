from subprocess import Popen, PIPE
from multiprocessing import Pool
import os
from enum import Enum
from lxml import etree
from lxml.etree import Element
import urllib.request
import json
import shutil
import gzip
import bsdiff4


from diachronic.conf import DEFAULT_PATH

URL_PREFIX = "http://dumps.wikimedia.your.org/enwiki/20170901/"
WIKI_PATH = DEFAULT_PATH + "enwiki-20170901-pages-meta-history27.xml-p42663462p42922763.7z"
OUTPUT_PATH = DEFAULT_PATH + "outfiles/"
WIKI_NS = "{http://www.mediawiki.org/xml/export-0.10/}"
REVISION_POWER = 1.1
MAX_REVISION = 1E9
BUFF_SIZE = 1E8
THREADS = 4


class Tags(Enum):
    Page = "page"
    Revision = "revision"
    Timestamp = "timestamp"
    Title = "title"
    Text = "text"

    @property
    def nstag(self) -> str:
        return WIKI_NS + self.value

    @staticmethod
    def ltag(elem: Element) -> str:
        return etree.QName(elem).localname


class Diachronic(object):
    def __init__(self):
        pass

    def wiki_files(self):
        json_url = urllib.request.urlopen("{}dumpstatus.json".format(URL_PREFIX))
        data = json.loads(json_url.read().decode())
        json_url.close()
        filenames = data["jobs"]["metahistory7zdump"]["files"].keys()
        pool = Pool()
        for filename in filenames:
            pool.apply_async(self.run, (filename, ))
        pool.close()
        pool.join()
        return 1

    @staticmethod
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

    def run(self, wiki_file):
        print("Downloading", wiki_file)
        response = urllib.request.urlopen(URL_PREFIX + wiki_file)
        download_file = open(DEFAULT_PATH + wiki_file, 'wb')
        shutil.copyfileobj(response, download_file)
        response.close()
        download_file.close()
        print("Downloaded", wiki_file)
        wiki_path = DEFAULT_PATH + wiki_file
        output_path = OUTPUT_PATH + wiki_file + ".gz"

        print("Running", wiki_path)
        global_buffer = bytes()
        local_buffer = bytes()
        current_revision = None
        rev_index = 0
        print("Making valid indices", wiki_path)
        valid_indexes = self.make_valid_indexes()
        print("Opening Popen", wiki_path)
        stdout = Popen(["7z", "e", "-so", wiki_path], stdout=PIPE).stdout
        starting = True

        with gzip.open(output_path, 'wb') as f:
            for event, elem in etree.iterparse(stdout):
                if starting:
                    print("Starting read", wiki_path)
                    starting = False
                tag = elem.tag
                if tag == Tags.Revision.nstag:
                    if rev_index in valid_indexes:
                        text = bytes(elem.find(Tags.Text.nstag).text or "", 'UTF-8')
                        if rev_index == 0:
                            current_revision = text
                            local_buffer += text
                        else:
                            local_buffer += bsdiff4.diff(current_revision, text)
                            current_revision = text
                    rev_index += 1
                elif tag == Tags.Title.nstag:
                    title = elem.text
                elif tag == Tags.Page.nstag:
                    global_buffer += bytes(title, 'UTF-8') + local_buffer
                    current_revision = None
                    rev_index = 0
                    local_buffer = bytes()

                    if len(global_buffer) > BUFF_SIZE:
                        f.write(global_buffer)
                        global_buffer = bytes()

                    while elem.getprevious() is not None:
                        del elem.getparent()[0]

                elem.clear()

            f.write(global_buffer)
        print("Finished running", wiki_path)
        os.remove(DEFAULT_PATH + wiki_file)
        return wiki_path



if __name__ == "__main__":
    d = Diachronic()
    d.wiki_files()
