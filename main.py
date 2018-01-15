import json
import os
import shutil
import urllib.request
import traceback
import logging
import psutil
from collections import defaultdict
from typing import List, Dict, Tuple
from multiprocessing import Semaphore, Pool
from subprocess import Popen, PIPE
from datetime import datetime, timedelta

from lxml import etree
from lxml.etree import Element
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

from diachronic import global_conf, Tags

PROCESS_MEM = psutil.virtual_memory().total / psutil.cpu_count()
# Fraction of (total_mem/cpu_count) that a given process uses before flushing buffer
PROCESS_MEM_LIMIT = .95

DOWNLOAD_SEMAPHORE = Semaphore(global_conf.download_parallelism)
FAILURES = []


def make_path(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path)


def get_wiki_from_filename(wiki_file: str) -> str:
    return wiki_file.split("-")[0]


class WikiHandler(object):
    def __init__(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        make_path(global_conf.input_path)

    def get_filenames(self) -> List[str]:
        filenames = []
        for wiki in global_conf.wikis:
            url_prefix = global_conf.get_url_prefix(wiki)
            url = "{}dumpstatus.json".format(url_prefix)
            logging.info("Grabbing filenames from {}".format(url))
            conn = urllib.request.urlopen(url)
            data = json.loads(conn.read().decode())
            conn.close()
            filenames.extend(list(data["jobs"]["metahistory7zdump"]["files"].keys()))
        return filenames

    def get_files_to_skip(self) -> List[str]:
        client = storage.Client()
        return [blob.name for blob in client.get_bucket(global_conf.bucket).list_blobs()]

    def get_files_to_run(self, overwrite=False) -> List[str]:
        all_filenames = self.get_filenames()
        if overwrite:
            logging.info("Overwrite enabled, running all {} files".format(len(all_filenames)))
            return all_filenames

        skipfiles = self.get_files_to_skip()
        files_to_run = [f for f in all_filenames
                        if "{}.{}".format(f, global_conf.output_suffix) not in skipfiles]
        skip_count = len(all_filenames) - len(files_to_run)
        logging.info("Running {} files and skipping {}".format(len(files_to_run), skip_count))
        return files_to_run

    def download(self, wiki_file: str) -> None:
        logging.info("Downloading {}".format(wiki_file))
        wiki = get_wiki_from_filename(wiki_file)
        url_prefix = global_conf.get_url_prefix(wiki)
        response = urllib.request.urlopen(url_prefix + wiki_file)
        download_file = open(global_conf.input_path + wiki_file, 'wb')
        shutil.copyfileobj(response, download_file)
        response.close()
        download_file.close()
        logging.info("Downloaded {}".format(wiki_file))

    def run_file(self, wiki_file: str) -> None:
        try:
            with DOWNLOAD_SEMAPHORE:
                self.download(wiki_file)
                parser = WikiFileParser(wiki_file)
            parser.run()
        except Exception:
            logging.info("Caught exception on {}".format(wiki_file))
            logging.error(traceback.format_exc())
            FAILURES.append(wiki_file)
            os.remove(global_conf.input_path + wiki_file)

    def run(self) -> None:
        logging.info("Running {}".format(global_conf.month_source))
        filenames_to_run = self.get_files_to_run()
        pool = Pool()
        pool.map_async(self.run_file, filenames_to_run, error_callback=self._on_error)
        pool.close()
        pool.join()
        logging.info("{} Run completed. Failures: {}".format(global_conf.month_source, FAILURES))

    def _on_error(self, ex: Exception):
        raise ex


class WikiFileParser(object):
    def __init__(self, wiki_file: str):
        self.arrow_cols = ("namespace", "title", "timestamp", "text")

        self.wiki_file = wiki_file
        self.wiki = get_wiki_from_filename(self.wiki_file)

        output_prefix = global_conf.get_output_prefix(self.wiki)
        make_path(global_conf.output_path + output_prefix)
        self.output_file = "{}{}.{}".format(output_prefix,
                                            self.wiki_file,
                                            global_conf.output_suffix)

        # State trackers
        self.arrow_buff = {colname: [] for colname in self.arrow_cols}
        self.arrow_row, self.cur_date, self.current_revision = self.iter_reset()

        self.schema: pq.ParquetSchema = None
        self.writer: pq.ParquetWriter = None

    def iter_reset(self) -> Tuple[Dict[str, None], datetime, None]:
        self.arrow_row = {colname: None for colname in self.arrow_cols}
        self.cur_date = global_conf.datetime_init
        self.current_revision = None
        return self.arrow_row, self.cur_date, self.current_revision

    @property
    def func_dict(self) -> Dict[str, callable]:
        d = {
            Tags.Revision.nstag: self.parse_revision,
            Tags.Namespace.nstag: self.parse_namespace,
            Tags.Page.nstag: self.parse_finish,
            Tags.Title.nstag: self.parse_title
        }
        return defaultdict(lambda: (lambda x: None), **d)

    def parse_title(self, elem: Element) -> None:
        self.arrow_row["title"] = elem.text

    def parse_namespace(self, elem: Element) -> None:
        self.arrow_row["namespace"] = elem.text

    def parse_revision(self, elem: Element) -> None:
        if self.arrow_row["namespace"] == "0":
            timestamp = datetime.strptime(elem.find(Tags.Timestamp.nstag).text[:-1], "%Y-%m-%dT%H:%M:%S")
            if timestamp >= self.cur_date:
                self.cur_date = datetime.combine(timestamp.date(), datetime.min.time()) + timedelta(days=1)
                text = elem.find(Tags.Text.nstag).text or ""
                self.arrow_row["text"] = text
                self.arrow_row["timestamp"] = timestamp
                for col, val in self.arrow_row.items():
                    self.arrow_buff[col].append(val)
        elem.clear()

    def parse_finish(self, elem: Element) -> None:
        self.iter_reset()

        # Determine whether buffer needs to be flushed based on available memory
        process = psutil.Process(os.getpid())
        if process.memory_info().rss / PROCESS_MEM >= PROCESS_MEM_LIMIT:
            self.write()
        elem.clear()

    def stream(self) -> None:
        stdout = Popen(["7z", "e", "-so", global_conf.input_path + self.wiki_file], stdout=PIPE).stdout
        for event, elem in etree.iterparse(stdout, huge_tree=True):
            self.func_dict[elem.tag](elem)

    def write(self) -> None:
        arrow_arrays = {colname: pa.array(arr) for colname, arr in self.arrow_buff.items()}
        arrow_table = pa.Table.from_arrays(arrays=list(arrow_arrays.values()), names=list(arrow_arrays.keys()))
        if not self.writer:
            self.writer = pq.ParquetWriter(global_conf.output_path + self.output_file,
                                           arrow_table.schema, compression='brotli')
        self.writer.write_table(arrow_table)
        self.arrow_buff = {colname: [] for colname in self.arrow_cols}

    def upload(self) -> None:
        client = storage.Client()
        bucket = client.get_bucket(global_conf.bucket)
        blob = bucket.blob(self.output_file)
        with open(global_conf.output_path + self.output_file, 'rb') as pq_file:
            blob.upload_from_file(pq_file)

    def cleanup(self) -> None:
        os.remove(global_conf.input_path + self.wiki_file)
        os.remove(global_conf.output_path + self.output_file)

    def run(self) -> None:
        logging.info("Started parsing {}".format(self.wiki_file))
        self.stream()
        # Clear leftover buffer
        self.write()
        self.writer.close()
        self.upload()
        self.cleanup()
        logging.info("Finished parsing {}".format(self.wiki_file))


if __name__ == "__main__":
    WikiHandler().run()
