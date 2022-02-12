import boto3
from collections import OrderedDict
import logging
import os
import pathlib
from pydantic import BaseModel, validate_arguments
from uuid import uuid4
from typing import Any, Dict, List, Optional, Tuple, Type
import time
from trp import Document, SelectionElement, Word

from ..cmd.utils import environ_or_required
from ..schema import record_to_schema, schema_to_model
from ..types import (
    BBox,
    CheckboxLabel,
    DocData,
    NumberLabel,
    TextLabel,
    TimestampLabel,
)
from .tool import Tool


def convert_textract_bbox(bbox, page_num) -> BBox:
    return BBox(
        top=bbox.top,
        left=bbox.left,
        height=bbox.height,
        width=bbox.width,
        page=page_num,
    )


def parse_field(field, page_num, log=None):
    if field.value is None:
        # We don't know the bounding box for the value, so there is no point
        # trying to define the field
        return None

    if len(set([type(c) for c in field.value.content])) != 1:
        if log:
            log.warning(
                "Field '%s' on page %d with value '%s' has multiple types: %s. Skipping...",
                field.key.text,
                page_num + 1,
                field.value.text,
                field.value.content,
            )
        return None

    if isinstance(field.value.content[0], SelectionElement):
        bbox = field.value.geometry.boundingBox
        assert field.value.text in ("SELECTED", "NOT_SELECTED")
        value = CheckboxLabel(
            value=field.value.text == "SELECTED",
            location=convert_textract_bbox(field.value.geometry.boundingBox, page_num),
        )
    elif isinstance(field.value.content[0], Word):
        # Textract only reports values as "text" types. There is no number parsing (as far as I can tell).
        # With typed systems like Impira, we rely on their additional information while creating fields
        # to create a more specific type.
        bbox = field.value.geometry.boundingBox
        value = TextLabel(
            value=field.value.text,
            location=convert_textract_bbox(field.value.geometry.boundingBox, page_num),
        )
    else:
        assert False, "Unknown value type: %s" % (field.value.content[0])

    return value


def doc_to_record(log, doc):
    fields = OrderedDict()
    for page_num, page in enumerate(doc.pages):
        for field in page.form.fields:
            value = parse_field(field, page_num)
            if value is not None:
                fields[field.key.text] = value

    # Construct a pydantic object for this record
    T = schema_to_model(record_to_schema(fields))
    return T(**fields)


class Textract(Tool):
    class Config(BaseModel):
        s3_bucket: Optional[str]
        s3_prefix: str

    @staticmethod
    def add_arguments(parser):
        parser.add_argument(
            "--s3-bucket",
            default=None,
            type=str,
            help="an existing s3 bucket to use for staging files. if not specified, impira cli will try to find an existing one or create one",
        )
        parser.add_argument(
            "--s3-prefix", **environ_or_required("TEXTRACT_S3_BUCKET", "")
        )

    @validate_arguments
    def __init__(self, config: Config):
        self.config = config

    def _init_bucket(self):
        if self.config.s3_bucket is not None:
            return

        log = self._log()
        session = boto3.Session()
        s3 = boto3.client("s3")

        matching_bucket = None
        for bucket in s3.list_buckets()["Buckets"]:
            if "impira-cli-staging-area" not in bucket["Name"]:
                continue
            location = s3.get_bucket_location(Bucket=bucket["Name"])[
                "LocationConstraint"
            ]
            if location is None or location == session.region_name:
                matching_bucket = bucket["Name"]
                break

        if matching_bucket is None:
            log.info(
                "No 'impira-cli-staging-area' bucket exists, going to try creating one..."
            )
            matching_bucket = "impira-cli-staging-area-" + str(uuid4())
            s3.create_bucket(Bucket=matching_bucket)
            log.info("Created bucket s3://%s", matching_bucket)
        else:
            log.info("Using existing bucket s3://%s", matching_bucket)

        self.config.s3_bucket = matching_bucket

    @validate_arguments
    def textract_document(self, fname: pathlib.Path, forms=True, tables=False):
        self._init_bucket()

        log = self._log()

        s3 = boto3.client("s3")
        key = os.path.join(self.config.s3_prefix, str(uuid4()), fname.name)
        s3.upload_file(str(fname), self.config.s3_bucket, key)

        feature_types = []
        if forms:
            feature_types.append("FORMS")
        if tables:
            feature_types.append("TABLES")

        try:
            textract = boto3.client("textract")
            job_info = textract.start_document_analysis(
                DocumentLocation={
                    "S3Object": {"Bucket": self.config.s3_bucket, "Name": key}
                },
                FeatureTypes=feature_types,
            )

            job_id = job_info["JobId"]

            response = None
            while response is None or response["JobStatus"] == "IN_PROGRESS":
                if response is not None:
                    log.info("Sleeping for 5 seconds...")
                    time.sleep(5)

                response = textract.get_document_analysis(JobId=job_id)

            assert response["JobStatus"] == "SUCCEEDED"

            log.debug("Reading textract results")
            pages = [response]
            while True:
                next_token = response.get("NextToken", None)
                if next_token is None:
                    break
                response = textract.get_document_analysis(
                    JobId=job_id, NextToken=next_token
                )
                pages.append(response)
        finally:
            log.debug("Cleaning up file on S3")
            s3.delete_object(Bucket=self.config.s3_bucket, Key=key)

        return Document(pages)

    @validate_arguments
    def process_document(self, fname: pathlib.Path, forms=True, tables=False):
        doc = self.textract_document(fname, forms, tables)

        return doc_to_record(self._log(), doc)
