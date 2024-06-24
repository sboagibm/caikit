# Copyright The Caikit Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This module defines the @schema decorator which can be used to declare data
model objects inline without manually defining the protobufs representation
"""


# Standard
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

# Third Party
from google.protobuf import message as _message
from google.protobuf.internal.enum_type_wrapper import EnumTypeWrapper
import pydantic

# First Party
from py_to_proto.dataclass_to_proto import DataclassConverter
import alog

# Local
from ..exceptions import error_handler
from . import enums
from .base import DataBase, _DataBaseMetaClass

from .json_dict import JsonDict

from google.protobuf import descriptor as _descriptor

from google.protobuf import struct_pb2

import numpy as np

from py_to_proto.dataclass_to_proto import PY_TO_PROTO_TYPES

## Globals #####################################################################

log = alog.use_channel("SCHEMA")
error = error_handler.get(log)

DATAOBJECT_PY_TO_PROTO_TYPES = {
    JsonDict: struct_pb2.Struct,
    np.int32: _descriptor.FieldDescriptor.TYPE_INT32,
    np.int64: _descriptor.FieldDescriptor.TYPE_INT64,
    np.uint32: _descriptor.FieldDescriptor.TYPE_UINT32,
    np.uint64: _descriptor.FieldDescriptor.TYPE_UINT64,
    np.float32: _descriptor.FieldDescriptor.TYPE_FLOAT,
    np.float64: _descriptor.FieldDescriptor.TYPE_DOUBLE,
    **PY_TO_PROTO_TYPES,
}


# Common package prefix
CAIKIT_DATA_MODEL = "caikit_data_model"

_USER_DEFINED_DEFAULTS = "__user_defined_defaults__"

# Registry of auto-generated protos so that they can be rendered to .proto
_AUTO_GEN_PROTO_CLASSES = []

DataObjectBase = pydantic.BaseModel

_DataObjectBaseT = TypeVar("_DataObjectBaseT", bound=Type[pydantic.BaseModel])


def dataobject(*args, **kwargs) -> Callable[[_DataObjectBaseT], _DataObjectBaseT]:
    def decorator(cls: _DataObjectBaseT) -> _DataObjectBaseT:
        # Make sure that the wrapped class does NOT inherit from DataBase
        error.value_check(
            "<COR95184230E>",
            issubclass(cls, (DataObjectBase, Enum)),
            "{} must inherit from DataObjectBase/Enum when using @dataobject",
            cls.__name__,
        )

        # Add the package to the kwargs
        kwargs.setdefault("package", package)

        # Return the decorated class
        return cls

    # If called without the function invocation, fill in the default argument
    if args and callable(args[0]):
        assert not kwargs, "This shouldn't happen!"
        package = CAIKIT_DATA_MODEL
        return decorator(args[0])

    # Pull the package as an arg or a keyword arg
    if args:
        package = args[0]
        if "package" in kwargs:
            raise TypeError("Got multiple values for argument 'package'")
    else:
        package = kwargs.get("package", CAIKIT_DATA_MODEL)
    return decorator


def render_dataobject_protos(interfaces_dir: str):
    """Write out protobufs files for all proto classes generated from dataobjects
    to the target interfaces directory

    Args:
        interfaces_dir (str): The target directory (must already exist)
    """
    # for proto_class in _AUTO_GEN_PROTO_CLASSES:
    #     proto_class.write_proto_file(interfaces_dir)
    # TODO: implement render_dataobject_protos
    raise AssertionError("render_dataobject_protos not yet implemented")


def make_dataobject(
    *,
    name: str,
    annotations: Dict[str, type],
    bases: Optional[Iterable[type]] = None,
    attrs: Optional[Dict[str, Any]] = None,
    proto_name: Optional[str] = None,
    **kwargs,
) -> pydantic.BaseModel:
    """Factory function for creating net-new dataobject classes

    WARNING: This is a power-user feature that should be used with caution since
        dynamically generated dataobject classes have portability issues due to
        the use of global registries.

    Kwargs:
        name (str): The name of the class to create
        annotations (Dict[str, type]): The type annotations for the class
        bases (Optional[Iterable[type]]): Additional base classes beyond
            DataObjectBase
        attrs (Optional[Dict[str, Any]]): Additional class attributes beyond
            __annotations__
        proto_name (Optional[str]): Alternate name to use for the name of
            protobuf message

    Returns:
        dataobject_class (_DataObjectBaseMetaClass): Programmatically created
            class derived from DataObjectBase with the given name and
            annotations
    """
    bases = (DataObjectBase,) + tuple(bases or ())
    attrs = {
        "__annotations__": annotations,
        **(attrs or {}),
    }
    if proto_name is not None:
        kwargs["name"] = proto_name
    return dataobject(**kwargs)(
        pydantic.BaseModel.__new__(
            pydantic.BaseModel,
            name=name,
            bases=bases,
            attrs=attrs,
        )
    )


## Implementation Details ######################################################


def _dataobject_to_proto(*args, **kwargs):
    kwargs.setdefault("type_mapping", DATAOBJECT_PY_TO_PROTO_TYPES)
    return _DataobjectConverter(*args, **kwargs).descriptor


class _DataobjectConverter(DataclassConverter):
    """Augment the dataclass converter to be able to pull descriptors from
    existing data objects
    """

    def get_concrete_type(self, entry: Any) -> Any:
        """Also include data model classes and enums as concrete types"""
        unwrapped = self._resolve_wrapped_type(entry)
        if (
            isinstance(unwrapped, type)
            and issubclass(unwrapped, DataBase)
            and entry.get_proto_class() is not None
        ) or hasattr(unwrapped, "_proto_enum"):
            return entry
        return super().get_concrete_type(entry)

    def get_descriptor(self, entry: Any) -> Any:
        """Unpack data model classes and enums to their descriptors"""
        entry = self._resolve_wrapped_type(entry)
        if isinstance(entry, type) and issubclass(entry, DataBase):
            return entry.get_proto_class().DESCRIPTOR
        proto_enum = getattr(entry, "_proto_enum", None)
        if proto_enum is not None:
            return proto_enum.DESCRIPTOR
        return super().get_descriptor(entry)

    def get_optional_field_names(self, entry: Any) -> List[str]:
        """Get the names of any fields which are optional. This will be any
        field that has a user-defined default or is marked as Optional[]
        """
        optional_fields = list(getattr(entry, _USER_DEFINED_DEFAULTS, {}))
        for field_name, field in entry.__dataclass_fields__.items():
            if (
                field_name not in optional_fields
                and self._is_python_optional(field.type) is not None
            ):
                optional_fields.append(field_name)
        return optional_fields

    @staticmethod
    def _is_python_optional(entry: Any) -> Any:
        """Detect if this type is a python optional"""
        if get_origin(entry) is Union:
            args = get_args(entry)
            return type(None) in args


def _get_all_enums(
    proto_class: Union[_message.Message, EnumTypeWrapper],
) -> List[EnumTypeWrapper]:
    """Given a generated proto class, recursively extract all enums"""
    all_enums = []
    if isinstance(proto_class, EnumTypeWrapper):
        all_enums.append(proto_class)
    else:
        for enum_descriptor in proto_class.DESCRIPTOR.enum_types:
            all_enums.append(getattr(proto_class, enum_descriptor.name))
        for nested_proto_descriptor in proto_class.DESCRIPTOR.nested_types:
            all_enums.extend(
                _get_all_enums(getattr(proto_class, nested_proto_descriptor.name))
            )

    return all_enums


def _make_data_model_class(proto_class: Type[_message.Message], cls):
    if issubclass(cls, DataObjectBase):
        _DataBaseMetaClass.parse_proto_descriptor(cls)

    # Recursively make all nested message wrappers
    for nested_message_descriptor in proto_class.DESCRIPTOR.nested_types:
        nested_message_name = nested_message_descriptor.name
        nested_proto_class = getattr(proto_class, nested_message_name)
        setattr(
            cls,
            nested_message_name,
            _make_data_model_class(
                nested_proto_class,
                _DataBaseMetaClass.__new__(
                    _DataBaseMetaClass,
                    name=nested_message_name,
                    bases=(DataBase,),
                    attrs={"_proto_class": getattr(proto_class, nested_message_name)},
                ),
            ),
        )
    for nested_enum_descriptor in proto_class.DESCRIPTOR.enum_types:
        setattr(
            cls,
            nested_enum_descriptor.name,
            getattr(enums, nested_enum_descriptor.name),
        )

    return cls


def _make_oneof_init(cls):
    """Helper to augment a defaulted dataclass __init__ to support kwargs for
    oneof fields
    """
    original_init = cls.__init__
    fields_to_oneofs = cls._fields_to_oneof
    oneofs_to_fields = cls._fields_oneofs_map

    def __init__(self, *args, **kwargs):
        new_kwargs = {}
        to_remove = []
        which_oneof = {}
        for field_name, val in kwargs.items():
            if oneof_name := fields_to_oneofs.get(field_name):
                oneof_pos_idx = list(cls.__dataclass_fields__.keys()).index(oneof_name)
                has_pos_val = len(args) > oneof_pos_idx
                if has_pos_val:
                    error(
                        "<COR09282193E>",
                        TypeError(
                            "Received conflicting oneof args/kwargs for {}/{}".format(
                                oneof_name,
                                field_name,
                            )
                        ),
                    )

                other_oneof_fields = (
                    field
                    for field in [oneof_name] + oneofs_to_fields[oneof_name]
                    if field != field_name
                )
                if any(field in kwargs for field in other_oneof_fields):
                    error(
                        "<COR59933157E>",
                        TypeError(
                            "Received multiple keyword arguments for oneof {}".format(
                                oneof_name,
                            )
                        ),
                    )
                new_kwargs[oneof_name] = val
                to_remove.append(field_name)
                which_oneof[oneof_name] = field_name

        for kwarg in to_remove:
            del kwargs[kwarg]
        kwargs.update(new_kwargs)
        original_init(self, *args, **kwargs)
        # noinspection PyProtectedMember
        setattr(self, _DataBaseMetaClass._WHICH_ONEOF_ATTR, which_oneof)

    return __init__


def _has_dataclass_init(cls) -> bool:
    """When the dataclass decorator adds an __init__ to a class, it adds
    __annotations__ to the init function itself. This function uses that fact to
    detect if the class's __init__ function was generated by @dataclass
    """
    return bool(getattr(cls.__init__, "__annotations__", None)) and not any(
        cls.__init__ is base.__init__ for base in cls.__bases__
    )
