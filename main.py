import json
import os
import shutil
import urllib.request
from typing import Set, List
from multiprocessing import Process, Semaphore, cpu_count, Pool
from subprocess import Popen, PIPE
from dateutil.parser import parse as dateparse
from datetime import datetime, timedelta
import logging

from lxml import etree
import pyarrow as pa
import pyarrow.parquet as pq
import bsdiff4
from google.cloud import storage

from diachronic import conf, Tags

DOWNLOAD_SEMAPHORE = Semaphore(conf["download"]["download_parallelism"])


class BatchFileHandler(object):
    def __init__(self):
        download = conf["download"]

        self.input_path = conf["download"]["input_path"]
        self.output_suffix = conf["upload"]["output_suffix"]
        self.bucket = conf["upload"]["gcloud_bucket"]

        self.url_prefix = "{}{}/".format(download["url_prefix"],
                                         download["month_source"])

        self.date_init = conf["run"]["datetime_init"]

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def get_filenames(self) -> Set[str]:
        json_url = urllib.request.urlopen("{}dumpstatus.json".format(self.url_prefix))
        data = json.loads(json_url.read().decode())
        json_url.close()
        return set(data["jobs"]["metahistory7zdump"]["files"].keys())

    def get_files_to_skip(self) -> Set[str]:
        client = storage.Client()
        return {blob.name for blob in client.get_bucket(self.bucket).list_blobs()}

    def get_files_to_run(self) -> Set[str]:
        return {f for f in self.get_filenames()
                if "{}.{}".format(f, self.output_suffix) in self.get_files_to_skip()}

    def download(self, wiki_file: str) -> None:
        logging.info("Downloading {}".format(wiki_file))
        response = urllib.request.urlopen(self.url_prefix + wiki_file)
        download_file = open(self.input_path + wiki_file, 'wb')
        shutil.copyfileobj(response, download_file)
        response.close()
        download_file.close()
        logging.info("Downloaded {}".format(wiki_file))

    def run_file(self, wiki_file):
        with DOWNLOAD_SEMAPHORE:
            self.download(wiki_file)
            parser = WikiParser(wiki_file)
        parser.run()

    def run(self):
        logging.info("Running")
        filenames_to_run = self.get_filenames()
        pool = Pool(4)
        for f in filenames_to_run:
            pool.apply_async(self.run_file, args=(f, ))
        pool.close()
        pool.join()

class WikiParser(object):
    def __init__(self, wiki_file):
        self.arrow_cols = ("namespace", "title", "initial_text",
                           "initial_timestamp", "diff_timestamps", "diffs")

        self.wiki_file = wiki_file

        self.datetime_init = conf["run"]["datetime_init"]
        self.bucket = conf["upload"]["gcloud_bucket"]
        self.input_path = conf["download"]["input_path"]
        self.output_path = conf["upload"]["output_path"]
        self.output_file = "{}.{}".format(self.wiki_file, conf["upload"]["output_suffix"])

        self.arrow_buff = {colname: [] for colname in self.arrow_cols}
        self.arrow_row, self.cur_date, self.current_revision = self.iter_reset()

    def iter_reset(self):
        self.arrow_row = {colname: None for colname in self.arrow_cols}
        self.cur_date = self.datetime_init
        self.current_revision = None
        return self.arrow_row, self.cur_date, self.current_revision

    @property
    def func_dict(self):
        return {
            Tags.Revision.nstag: self.parse_revision,
            Tags.Namespace.nstag: self.parse_namespace,
            Tags.Page.nstag: self.parse_page,
        }

    def parse_revision(self, elem) -> None:
        timestamp = dateparse(elem.find(Tags.Timestamp.nstag).text, ignoretz=True)
        if timestamp >= self.cur_date and self.arrow_row["namespace"] == "0":
            self.cur_date = datetime.combine(timestamp.date(), datetime.min.time()) + timedelta(days=1)
            text = elem.find(Tags.Text.nstag).text or ""
            text_bytes = bytes(text, "UTF-8")
            if not self.current_revision:
                self.current_revision = text_bytes
                self.arrow_row["initial_text"] = text
                self.arrow_row["initial_timestamp"] = timestamp
                self.arrow_row["diff_timestamps"] = []
                self.arrow_row["diffs"] = []
            else:
                self.arrow_row["diffs"].append(bsdiff4.diff(self.current_revision, text_bytes))
                self.arrow_row["diff_timestamps"].append(timestamp)
                self.current_revision = text_bytes
            elem.clear()

    def parse_namespace(self, elem) -> None:
        self.arrow_row["namespace"] = elem.text

    def parse_page(self, elem) -> None:
        self.arrow_row["title"] = elem.find(Tags.Title.nstag).text
        if self.arrow_row["namespace"] == '0':
            for col, val in self.arrow_row.items():
                self.arrow_buff[col].append(val)
        self.iter_reset()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

        elem.clear()

    def write(self):
        arrow_arrays = {colname: pa.array(arr) for colname, arr in self.arrow_buff.items()}
        arrow_table = pa.Table.from_arrays(arrays=list(arrow_arrays.values()), names=list(arrow_arrays.keys()))
        pq.write_table(arrow_table, self.output_path + self.output_file, compression='brotli')

    def upload(self):
        client = storage.Client()
        bucket = client.get_bucket(self.bucket)
        blob = bucket.blob(self.output_file)
        with open(self.output_path + self.output_file, 'rb') as pq_file:
            blob.upload_from_file(pq_file)

    def cleanup(self):
        os.remove(self.input_path + self.wiki_file)
        os.remove(self.output_path + self.output_file)

    def stream(self):
        stdout = Popen(["7z", "e", "-so", self.input_path + self.wiki_file], stdout=PIPE).stdout
        for event, elem in etree.iterparse(stdout, huge_tree=True):
            self.func_dict.get(elem.tag, lambda x: None)(elem)

    def run(self):
        logging.info("Running {}".format(self.wiki_file))
        self.stream()
        self.write()
        self.upload()
        self.cleanup()


if __name__ == "__main__":
    handler = BatchFileHandler()
    handler.run()
