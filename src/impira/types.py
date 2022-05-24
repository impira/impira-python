from datetime import datetime, date
from enum import Enum
import pathlib
from pydantic import BaseModel, validate_arguments, StrictFloat, StrictInt
import random
from typing import Any, ForwardRef, Dict, List, Optional, Union, Callable

from . import fmt

# All values in the bbox are based on percentages
class BBox(BaseModel):
    top: float = 0
    left: float = 0
    height: float = 0
    width: float = 0
    page: int = 0

    def expand(self):
        return BBox(
            top=self.top - 0.005,
            left=self.left - 0.005,
            height=self.height + 0.005,
            width=self.width + 0.005,
            page=self.page,
        )


@validate_arguments
def combine_bboxes(*boxes: BBox) -> BBox:
    pages = set([b.page for b in boxes])
    assert len(pages) == 1, "All boxes must be on the same page: %s" % (pages)
    top = min([b.top for b in boxes])
    left = min([b.left for b in boxes])
    height = max([b.top + b.height for b in boxes]) - top
    width = max([b.left + b.width for b in boxes]) - left
    return BBox(
        top=top,
        left=left,
        height=height,
        width=width,
        page=list(pages)[0],
    )


class Cell(BaseModel):
    row: int
    column: int


class ScalarLabel(BaseModel):
    value: Any
    location: Optional[BBox]
    cell: Optional[Cell]

    def fmt(self):
        return self.value

    def u_fmt(self):
        return self.fmt()


class NumberLabel(ScalarLabel):
    value: Optional[Union[StrictInt, StrictFloat, float]]


class TextLabel(ScalarLabel):
    value: Optional[str]


class DocumentTagLabel(ScalarLabel):
    value: Optional[str]


class TimestampLabel(ScalarLabel):
    value: Optional[datetime]

    @staticmethod
    @validate_arguments
    def from_date(date: Optional[date], **kwargs):
        return TimestampLabel(value=datetime.combine(date, datetime.min.time()) if date else None, **kwargs)

    # TODO: support more timestamp formatting options
    def fmt(self):
        return fmt.american_date(self.value) if self.value is not None else ""

    def u_fmt(self):
        return fmt.unambiguous_date(self.value) if self.value is not None else ""


class CheckboxLabel(ScalarLabel):
    value: Optional[bool]

    def fmt(self):
        return "\u2717" if self.value else ""

    def u_fmt(self):
        return "true" if self.value else "false"


class SignatureLabel(ScalarLabel):
    value: Optional[bool]


def traverse(record: Any, fn: Callable[[Any], None]):
    if isinstance(record, ScalarLabel):
        fn(record)
    elif isinstance(record, list):
        for row in record:
            traverse(row, fn)
    else:
        for v in dict(record).values():
            traverse(v, fn)


class DocData(BaseModel):
    fname: pathlib.Path = pathlib.Path("")
    url: Optional[str]
    record: Any = None


DocSchema = ForwardRef("DocSchema")


class DocSchema(BaseModel):
    # NOTE: This schema does not support nested objects (only lists, i.e. tables)
    fields: Dict[str, Union[DocSchema, str]]


DocSchema.update_forward_refs()


class DocManifest(BaseModel):
    doc_schema: DocSchema
    docs: List[DocData]
