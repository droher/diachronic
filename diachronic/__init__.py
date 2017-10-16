import yaml
from enum import Enum

with open("conf.yml", 'rb') as f:
    conf = yaml.load(f)


class Tags(Enum):
    Page = "page"
    Revision = "revision"
    Timestamp = "timestamp"
    Title = "title"
    Text = "text"
    Namespace = "ns"

    @property
    def nstag(self) -> str:
        return conf["run"]["namespace"] + self.value
