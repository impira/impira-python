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

    src_collection_uid = conn.get_collection_uid(args.src_collection_name)
    if src_collection_uid is None:
        logging.fatal("Could not find collection named '%s'", args.src_collection_name)
        exit(1)

    if len(conn.query('@file_collections name="%s"' % (args.dst_collection_name))["data"]) != 0:
        logging.warning(
            "Destination collection '%s' already exists. Will add fields to the existing collection.",
            args.dst_collection_name,
        )
        dst_collection_uid = conn.get_collection_uid(args.dst_collection_name)
    else:
        logging.info("Creating collection '%s'...", args.dst_collection_name)
        dst_collection_uid = conn.create_collection(args.dst_collection_name)
        logging.info(
            "You can visit the new collection (%s) at: %s"
            % (args.dst_collection_name, conn.get_app_url("fc", dst_collection_uid))
        )

    logging.info("Importing fields...")
    conn.import_fields(dst_collection_uid, src_collection_uid)
    logging.info("Done!")
