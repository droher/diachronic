import json
import os
import shutil
import urllib.request
from enum import Enum
from multiprocessing import Pool
from subprocess import Popen, PIPE
from timeit import default_timer as timer
from dateutil.parser import parse as dateparse
from datetime import datetime, timedelta

from lxml import etree
from lxml.etree import Element
import pyarrow as pa
import pyarrow.parquet as pq
import bsdiff4
from google.cloud import storage

from diachronic.conf import DEFAULT_PATH

DATE_INIT = datetime(2001, 1, 15, 0, 0, 0)
MONTH = '20171001'
URL_PREFIX = "http://dumps.wikimedia.your.org/enwiki/{}/".format(MONTH)
OUTPUT_PATH = DEFAULT_PATH + "outfiles/"
WIKI_NS = "{http://www.mediawiki.org/xml/export-0.10/}"
REVISION_POWER = 1.1
MAX_REVISION = 1E9
BUFF_SIZE = 1E8
BUCKET_NAME = ""


class Tags(Enum):
    Page = "page"
    Revision = "revision"
    Timestamp = "timestamp"
    Title = "title"
    Text = "text"
    Namespace = "ns"

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
        filenames = list(data["jobs"]["metahistory7zdump"]["files"].keys())
        pool = Pool()
        client = storage.Client()
        bucket = client.get_bucket(BUCKET_NAME)

        for filename in filenames:
            if not bucket.blob(filename + ".parquet").exists():
                pool.apply_async(self.run, (filename, ))
            else:
                print("Skipping", filename)
        pool.close()
        pool.join()
        return 1

    def run(self, wiki_file):
        start = timer()
        response = urllib.request.urlopen(URL_PREFIX + wiki_file)
        download_file = open(DEFAULT_PATH + wiki_file, 'wb')
        shutil.copyfileobj(response, download_file)
        response.close()
        download_file.close()
        end = timer()
        print("Downloaded {} in {} minutes".format(wiki_file, round((end - start) / 60, 2)))

        start = timer()
        wiki_path = DEFAULT_PATH + wiki_file
        output_filename = wiki_file + ".parquet"

        # Parquet Table Columns
        arrow_cols = ("namespace", "title", "initial_text",
                      "initial_timestamp", "diff_timestamps", "diffs")
        arrow_buff = {colname: [] for colname in arrow_cols}

        # Tracking iterations
        arrow_row = {colname: None for colname in arrow_cols}
        current_revision = None
        cur_date = DATE_INIT

        stdout = Popen(["7z", "e", "-so", wiki_path], stdout=PIPE).stdout
        for event, elem in etree.iterparse(stdout):
            tag = elem.tag
            if tag == Tags.Revision.nstag:
                timestamp = dateparse(elem.find(Tags.Timestamp.nstag).text, ignoretz=True)
                if timestamp >= cur_date and arrow_row["namespace"] == "0":
                    cur_date = datetime.combine(timestamp.date(), datetime.min.time()) + timedelta(days=1)
                    text = elem.find(Tags.Text.nstag).text or ""
                    text_bytes = bytes(text, "UTF-8")
                    if not current_revision:
                        current_revision = text_bytes
                        arrow_row["initial_text"] = text
                        arrow_row["initial_timestamp"] = timestamp
                        arrow_row["diff_timestamps"] = []
                        arrow_row["diffs"] = []
                    else:
                        arrow_row["diffs"].append(bsdiff4.diff(current_revision, text_bytes))
                        arrow_row["diff_timestamps"].append(timestamp)
                        current_revision = text_bytes
                elem.clear()
            elif tag == Tags.Namespace.nstag:
                arrow_row["namespace"] = elem.text
            elif tag == Tags.Page.nstag:
                arrow_row["title"] = elem.find(Tags.Title.nstag).text
                # Write to buffer and reset trackers
                if arrow_row["namespace"] == '0':
                    for col, val in arrow_row.items():
                        arrow_buff[col].append(val)
                arrow_row = {colname: None for colname in arrow_cols}
                current_revision = None
                cur_date = DATE_INIT

                while elem.getprevious() is not None:
                    del elem.getparent()[0]

                elem.clear()

        arrow_arrays = {colname: pa.array(arr) for colname, arr in arrow_buff.items()}
        arrow_table = pa.Table.from_arrays(arrays=list(arrow_arrays.values()), names=list(arrow_arrays.keys()))
        pq.write_table(arrow_table, OUTPUT_PATH + output_filename, compression='brotli')

        # Cloud storage
        client = storage.Client()
        bucket = client.get_bucket(BUCKET_NAME)
        blob = bucket.blob(output_filename)
        with open(OUTPUT_PATH + output_filename, 'rb') as pq_file:
            blob.upload_from_file(pq_file)

        end = timer()
        os.remove(DEFAULT_PATH + wiki_file)
        os.remove(OUTPUT_PATH + output_filename)
        print("Finished {} with shape {} in {} minutes".format(wiki_file,
                                                               arrow_table.shape, round((end - start) / 60, 2)))
        return wiki_file


if __name__ == "__main__":
    d = Diachronic()
    d.wiki_files()
