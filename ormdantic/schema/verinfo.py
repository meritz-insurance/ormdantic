from typing import Dict, Any
import dataclasses
from datetime import datetime

# we will use version number for _valid_start and _valid_end fields. 
# this approach make simpler code than the period time.
# version has some restriction. The biggest one is that we cannot change 
# data as of past, because we cannot allocation the version between existed two 
# consequence version.

@dataclasses.dataclass(repr=True)
class VersionInfo():
    version: int | None = None
    who: str = 'system'
    why: str = ''
    when: datetime | None = None
    where: str = ''
    tag: str = ''
    revert: bool = False

    @staticmethod
    def create(who:str = 'system', why:str='', where:str = 'system', tag:str = '', revert:bool = False):
        return VersionInfo(None, who, why, None, where, tag)

    @staticmethod
    def from_dict(data:Dict[str, Any]):
        return VersionInfo(data['version'], data['who'], data['why'], 
                           data['when'], data['where'], data['tag'], data['revert'])

