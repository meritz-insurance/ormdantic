from typing import ForwardRef, Generic, List, TypeVar, Tuple

import pytest

from ormdantic.schema.base import PartOfMixin, PersistentModel
from ormdantic.util import (
    get_base_generic_type_of, get_type_args, get_mro_with_generic, 
    resolve_forward_ref, update_forward_refs_in_generic_base, is_derived_from, is_collection_type_of
)

T = TypeVar('T')


def test_get_base_generic_type_of():
    class GenericTest(PartOfMixin[PersistentModel]):
        pass

    base_generic = get_base_generic_type_of(GenericTest, PartOfMixin)
    assert PartOfMixin[PersistentModel] == base_generic

    base_generic = get_base_generic_type_of(GenericTest, PartOfMixin[PersistentModel])
    assert PartOfMixin[PersistentModel] == base_generic

    assert get_type_args(base_generic)[0]

    class MultipleInheritanceTest(List[str], GenericTest):
        pass

    base_generic = get_base_generic_type_of(MultipleInheritanceTest, PartOfMixin)

    assert PartOfMixin[PersistentModel] == base_generic

    base_generic = get_base_generic_type_of(MultipleInheritanceTest, list)

    assert List[str] == base_generic


def test_get_mro_with_generic():
    class Item(Generic[T]):
        pass

    class Items(Item[str]):
        pass

    assert [Items, Item[str], Generic, object] == list(get_mro_with_generic(Items))

    with pytest.raises(TypeError):
        get_mro_with_generic(0)


def test_update_forward_refs_in_generic_base():
    class Item():
        pass

    class Base(Generic[T]):
        pass

    class Derived(Base['Item']):
        pass

    update_forward_refs_in_generic_base(Derived, locals())

    base_type = get_base_generic_type_of(Derived, Base)

    assert (Item,) == get_type_args(base_type)


def test_resolve_forward_ref():
    class Item():
        pass

    assert Item == resolve_forward_ref(ForwardRef("Item"), locals())
    assert Item == resolve_forward_ref(Item, locals())
    

def test_is_derived_from():
    class Item():
        pass

    class Base(Generic[T]):
        pass

    class Derived(Base['Item']):
        pass


    assert is_derived_from(Item, Item)
    assert not is_derived_from(str, Item)

    assert is_derived_from(Derived, Base)
    assert not is_derived_from(Base, Derived)


def test_is_collection_type_of():
    assert is_collection_type_of(List[str], str)
    assert is_collection_type_of(Tuple[str], str)
    assert is_collection_type_of(Tuple[str, str], str)
    assert is_collection_type_of(Tuple[str, ...], str)
    assert is_collection_type_of(Tuple[str, int], (str, int))

    assert not is_collection_type_of(List[str], int)
    assert not is_collection_type_of(int, (int,))