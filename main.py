import json
import os
import shutil
import urllib.request
from typing import List
from multiprocessing import Semaphore, Pool
from subprocess import Popen, PIPE
from datetime import datetime, timedelta
import logging
import gc
import psutil

from lxml import etree
import pyarrow as pa
import pyarrow.parquet as pq
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

    def get_filenames(self) -> List[str]:
        json_url = urllib.request.urlopen("{}dumpstatus.json".format(self.url_prefix))
        data = json.loads(json_url.read().decode())
        json_url.close()
        return list(data["jobs"]["metahistory7zdump"]["files"].keys())

    def get_files_to_skip(self) -> List[str]:
        client = storage.Client()
        return [blob.name for blob in client.get_bucket(self.bucket).list_blobs()]

    def get_files_to_run(self, overwrite=False) -> List[str]:
        if overwrite:
            return self.get_filenames()
        return [f for f in self.get_filenames()
                if "{}.{}".format(f, self.output_suffix) in self.get_files_to_skip()]

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
        filenames_to_run = self.get_files_to_run(overwrite=True)
        pool = Pool()
        for f in filenames_to_run:
            pool.apply_async(self.run_file, args=(f, ))
        pool.close()
        pool.join()


class WikiParser(object):
    def __init__(self, wiki_file):
        self.arrow_cols = ("namespace", "title", "timestamp", "text")

        self.wiki_file = wiki_file

        self.datetime_init = conf["run"]["datetime_init"]
        self.bucket = conf["upload"]["gcloud_bucket"]
        self.input_path = conf["download"]["input_path"]
        self.output_path = conf["upload"]["output_path"]
        self.output_file = "{}.{}".format(self.wiki_file, conf["upload"]["output_suffix"])

        self.arrow_buff = {colname: [] for colname in self.arrow_cols}
        self.arrow_row, self.cur_date, self.current_revision = self.iter_reset()
        self.buf_size = 0

        self.schema = None
        self.writer = None

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
            Tags.Title.nstag: self.parse_title
        }

    def parse_title(self, elem) -> None:
        self.arrow_row["title"] = elem.text

    def parse_namespace(self, elem) -> None:
        self.arrow_row["namespace"] = elem.text

    def parse_revision(self, elem) -> None:
        if self.arrow_row["namespace"] == "0":
            timestamp = datetime.strptime(elem.find(Tags.Timestamp.nstag).text[:-1], "%Y-%m-%dT%H:%M:%S")
            if timestamp >= self.cur_date:
                self.cur_date = datetime.combine(timestamp.date(), datetime.min.time()) + timedelta(days=1)
                text = elem.find(Tags.Text.nstag).text or ""
                self.arrow_row["text"] = text
                self.arrow_row["timestamp"] = timestamp
                for col, val in self.arrow_row.items():
                    self.buf_size += len(str(val))
                    self.arrow_buff[col].append(val)
        elem.clear()

    def parse_page(self, elem) -> None:
        self.iter_reset()
        if self.buf_size >= 1E9:
            self.write()
        elem.clear()

    def stream(self):
        stdout = Popen(["7z", "e", "-so", self.input_path + self.wiki_file], stdout=PIPE).stdout
        for event, elem in etree.iterparse(stdout, huge_tree=True):
            self.func_dict.get(elem.tag, lambda x: None)(elem)

    def write(self):
        arrow_arrays = {colname: pa.array(arr) for colname, arr in self.arrow_buff.items()}
        arrow_table = pa.Table.from_arrays(arrays=list(arrow_arrays.values()), names=list(arrow_arrays.keys()))
        if not self.writer:
            self.writer = pq.ParquetWriter(self.output_path + self.output_file,
                                           arrow_table.schema, compression='brotli')
        self.writer.write_table(arrow_table)
        self.arrow_buff = {colname: [] for colname in self.arrow_cols}
        self.buf_size = 0
        gc.collect()

    def upload(self):
        client = storage.Client()
        bucket = client.get_bucket(self.bucket)
        blob = bucket.blob(self.output_file)
        with open(self.output_path + self.output_file, 'rb') as pq_file:
            blob.upload_from_file(pq_file)

    def cleanup(self):
        os.remove(self.input_path + self.wiki_file)
        os.remove(self.output_path + self.output_file)

    def run(self):
        logging.info("Started parsing {}".format(self.wiki_file))
        self.stream()
        self.write() # Clear leftover buffer
        self.writer.close()
        self.upload()
        self.cleanup()
        logging.info("Finished parsing {}".format(self.wiki_file))


if __name__ == "__main__":
    b = BatchFileHandler()
    b.run()
