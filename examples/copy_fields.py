#!/usr/bin/env python3

import argparse
import os
from urllib.parse import urlparse
import string
import json
import impira
from impira import InferredFieldType, FieldType, FieldSpec
from impira import schema
from typing import Any, Dict, List, Optional, Tuple, Type, Union
from impira.tools.impira import filter_inferred_fields, fields_to_doc_schema, generate_schema
from enum import Enum
from pydantic import BaseModel, validate_arguments
import logging

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--org-name", type=str, default=None, required=True)
    parser.add_argument("--api-token", type=str, default=None, required=True)
    parser.add_argument("src_collection_name", type=str)
    parser.add_argument("dst_collection_name", type=str)

    args = parser.parse_args()

    conn = impira.Impira(org_name=args.org_name, api_token=args.api_token)

    collections = conn.query('@file_collections name="%s"' % (args.src_collection_name))["data"]
    if len(collections) == 0:
        logging.fatal("Could not find collection named '%s'", args.src_collection_name)
        exit(1)
    if len(collections) > 1:
        logging.fatal("Multiple collections named '%s'", args.src_collection_name)
        exit(1)

    src_ec = collections[0]["field_ec"]

    if len(conn.query('@file_collections name="%s"' % (args.dst_collection_name))["data"]) != 0:
        logging.fatal(
            "Destination collection '%s' already exists. Please pick a differet name", args.dst_collection_name
        )
        exit(1)

    src_fields = [
        f
        for f in conn.query("@`%s` limit:0" % (src_ec))["schema"]["children"]
        if f["name"] not in ("File", "__system", "uid")
        and "comment" in f
        and json.loads(f["comment"])["entity_class"] == src_ec
    ]

    src_doc_schema = fields_to_doc_schema(filter_inferred_fields(src_fields))
    schema = generate_schema(src_doc_schema)
    field_specs = [f.field_type.build_field_spec(f.name, f.path) for f in schema]

    logging.info("Creating collection '%s'...", args.dst_collection_name)
    new_collection_uid = conn.create_collection(args.dst_collection_name)
    print(
        "You can visit the new collection (%s) at: %s"
        % (args.dst_collection_name, conn.get_app_url("fc", new_collection_uid))
    )

    logging.info("Creating %d fields...", len(field_specs))
    conn.create_fields(new_collection_uid, field_specs)
