import yaml
from datetime import datetime
from typing import List


class GlobalConf(object):
    def __init__(self):
        with open("./conf.yml", 'rb') as f:
            conf = yaml.load(f)

        self.wikis: List[str] = conf["wiki_info"]["wikis"]
        self.month_source: str = conf["wiki_info"]["month_source"]
        self.namespace: str = conf["wiki_info"]["namespace"]
        self.datetime_init: datetime = conf["wiki_info"]["datetime_init"]

        self.download_parallelism: int = conf["download"]["download_parallelism"]
        self.input_path: str = conf["download"]["input_path"]
        self.url_prefix: str = conf["download"]["url_prefix"]

        self.bucket: str = conf["upload"]["gcloud_bucket"]
        self.output_path: str = conf["upload"]["output_path"]
        self.output_suffix: str = conf["upload"]["output_suffix"]

    def get_url_prefix(self, wiki: str) -> str:
        return "{}{}/{}/".format(self.url_prefix, wiki, self.month_source)

    def get_output_prefix(self, wiki: str) -> str:
        return "{}/{}/".format(wiki, self.month_source)
