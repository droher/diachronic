from diachronic.conf import GlobalConf
from enum import Enum

global_conf = GlobalConf()


class Tags(Enum):
    Page = "page"
    Revision = "revision"
    Timestamp = "timestamp"
    Title = "title"
    Text = "text"
    Namespace = "ns"

    @property
    def nstag(self) -> str:
        return global_conf.namespace + self.value
