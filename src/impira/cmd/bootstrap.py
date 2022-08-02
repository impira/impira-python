import json
import pathlib
from shutil import copyfile
from uuid import uuid4

from ..tools.impira import Impira
from ..config import get_logger
from ..schema import schema_to_model
from ..types import DocData, DocManifest
from .utils import environ_or_required

log = get_logger("bootstrap")


def build_parser(subparsers, parent_parser):
    parser = subparsers.add_parser(
        "bootstrap",
        help="setup Impira using an existing document file and schema",
        parents=[parent_parser],
    )
    parser.add_argument(
        "--data",
        "-d",
        required=True,
        type=str,
        help="Directory to retrieve documents. This directory should contain one or more documents and a manifest (manifest.json)",
    )

    collection_args = parser.add_mutually_exclusive_group()
    collection_args.add_argument("--collection-prefix", **environ_or_required("IMPIRA_COLLECTION_PREFIX", "impira-cli"))
    collection_args.add_argument(
        "--collection",
        default=None,
        type=str,
        help="uid of an existing collection to use",
    )
    collection_args.add_argument(
        "--name",
        default=None,
        type=str,
        help="optional name for the new collection to create",
    )
    parser.add_argument(
        "--skip-upload",
        default=False,
        action="store_true",
        help="Skip uploading files into the collection",
    )
    parser.add_argument(
        "--add-files",
        default=False,
        action="store_true",
        help="Add missing files into the collection",
    )
    parser.add_argument(
        "--skip-missing-files",
        default=False,
        action="store_true",
        help="Skip missing files while labeling the collection",
    )
    parser.add_argument(
        "--skip-type-inference",
        default=False,
        action="store_true",
        help="Do not use Impira's type inference to select field types",
    )
    parser.add_argument(
        "--skip-new-fields",
        default=False,
        action="store_true",
        help="Only label existing fields in the collection",
    )
    parser.add_argument(
        "--label-empty-values",
        default=False,
        action="store_true",
        help="If labels are missing, assign them 'No value' labels",
    )
    parser.add_argument(
        "--max-fields",
        default=-1,
        type=int,
        help="Only create up to this many fields",
    )
    parser.add_argument(
        "--first-file",
        default=-0,
        type=int,
        help="Start with this file (indexed by 0). Useful for pagination.",
    )
    parser.add_argument(
        "--max-files",
        default=-1,
        type=int,
        help="Only upload up to this many files",
    )
    parser.add_argument(
        "--batch-size",
        default=50,
        type=int,
        help="Batch size for downloading text and labeling files",
    )
    parser.add_argument(
        "--first-batch",
        default=0,
        type=int,
        help="First batch to start (this is useful for resuming)",
    )
    Impira.add_arguments(parser)

    parser.set_defaults(func=main)
    return parser


def main(args):
    workdir = pathlib.Path(args.data)

    manifest_file = workdir / "manifest.json"

    if not manifest_file.exists():
        log.fatal("No manifest.json file found in %s", workdir)

    manifest = DocManifest.parse_file(manifest_file)
    M = schema_to_model(manifest.doc_schema)

    all_docs = []
    for doc in manifest.docs:
        doc.fname = workdir / doc.fname
        if doc.url is not None or doc.fname.exists():
            all_docs.append(doc)
            if doc.record is not None:
                doc.record = M.parse_obj(doc.record)

    tool = Impira(config=Impira.Config(**vars(args)))
    tool.run(
        manifest.doc_schema,
        manifest.docs,
        collection_prefix=args.collection_prefix,
        parallelism=args.parallelism,
        existing_collection_uid=args.collection,
        skip_type_inference=args.skip_type_inference,
        skip_upload=args.skip_upload,
        skip_missing_files=args.skip_missing_files,
        add_files=args.add_files,
        skip_new_fields=args.skip_new_fields,
        empty_labels=args.label_empty_values,
        collection_name=args.name,
        max_fields=args.max_fields,
        first_file=args.first_file,
        max_files=args.max_files,
        batch_size=args.batch_size,
        first_batch=args.first_batch,
        cache_dir=workdir / "cache",
    )
