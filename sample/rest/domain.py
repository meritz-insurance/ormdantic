from typing import Tuple
from pydantic import Field

import ormdantic as od
from datetime import date 

class Person(od.IdentifiedModel, od.TypeNamedModel):
	name: str
	birth: date


class PersonReference(od.StringReference[Person]):
	_target_field = 'id'


class Member(od.PersistentModel, od.TypeNamedModel, od.PartOfMixin['Company']):
	person: PersonReference
	join_at: date


class Flag(od.PersistentSharedContentModel, od.TypeNamedModel):
	color: str


class Company(od.IdentifiedModel, od.TypeNamedModel):
	name: str
	address: str

	flags: Tuple[od.ContentReferenceModel[Flag]] = Field(default=tuple())
	members: Tuple[Member,...] = Field(default=tuple())


od.update_forward_refs(Member, locals())