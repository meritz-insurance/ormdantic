from pydantic import BaseModel
from datetime import date 

class Member(BaseModel):
    name:str
    birth:date


member = Member(name='Steve Jobs', birth=date(1995, 2, 24))
assert '{"name": "Steve Jobs", "birth": "1995-02-24"}' == member.json()

member = Member.parse_raw('{"name": "Steve Jobs", "birth": "1955-02-24"}')
assert Member(name='Steve Jobs', birth=date(1955, 2, 24)) == member
