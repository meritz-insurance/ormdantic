from typing import Dict, Any
import dataclasses
from datetime import datetime

@dataclasses.dataclass(repr=True)
class VersionInfo():
    version: int | None = None
    who: str = 'system'
    why: str = ''
    when: datetime | None = None
    where: str = ''
    tag: str = ''

    @staticmethod
    def create(who:str = 'system', why:str='', where:str = 'system', tag:str = ''):
        return VersionInfo(None, who, why, None, where, tag)

    @staticmethod
    def from_dict(data:Dict[str, Any]):
        return VersionInfo(data['version'], data['who'], data['why'], data['when'], data['where'], data['tag'])
