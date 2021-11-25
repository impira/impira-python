from pydantic import BaseModel, create_model, validate_arguments
from typing import Any, Dict, List, Optional
from uuid import uuid4

from . import types
from .types import DocSchema


@validate_arguments
def record_to_schema(record) -> DocSchema:
    fields = {}
    for field_name, value in dict(record).items():
        if isinstance(value, List):
            fields[field_name] = record_to_schema(value[0])
        elif isinstance(value, Dict):
            assert False, "Unsupported: nested (object) fields"
        else:
            fields[field_name] = type(value).__name__

    return DocSchema(fields=fields)


@validate_arguments
def schema_to_model(s: DocSchema) -> BaseModel:
    fields = {}
    for field_name, field_schema in s.fields.items():
        if isinstance(field_schema, DocSchema):
            nested_type = List[schema_to_model(field_schema)]
            default_value = []
        else:
            nested_type = getattr(types, field_schema)

        fields[field_name] = (Optional[nested_type], None)

    return create_model(str(uuid4()), **fields)
