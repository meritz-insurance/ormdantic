from typing import ( Generic )
from pydantic import PrivateAttr

from ..util import get_logger

from .base import (
    ModelT, SchemaBaseModel
)

_logger = get_logger(__name__)

class ModelWithShadow(SchemaBaseModel, Generic[ModelT]):
    ''' identified by content '''
    _shadow : ModelT | None = PrivateAttr(None)

    @property
    def shadow(self) -> ModelT | None:
        return self._shadow

    @shadow.setter
    def shadow(self, data:ModelT):
        self._shadow = data

    class Config:
        title = 'model has the shadow field which is not saved.'

    

