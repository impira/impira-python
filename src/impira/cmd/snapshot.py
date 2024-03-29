import pathlib
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import requests

from ..api_v2 import urljoin
from ..config import get_logger
from ..tools.impira import Impira
from ..types import DocManifest, DocSchema
from .utils import add_datadir_arg
from ..credentials import Credentials


log = get_logger("snapshot")


def build_parser(subparsers, parent_parser):
    parser = subparsers.add_parser(
        "snapshot",
        help="Snapshot an Impira collection (fields, documents, and labels)",
        parents=[parent_parser],
    )
    Impira.add_arguments(parser)

    add_datadir_arg(parser)

    parser.add_argument(
        "--collection",
        default=[],
        nargs="*",
        help="uid of the collection to snapshot",
    )

    parser.add_argument(
        "--all-collections",
        default=False,
        action="store_true",
        help="Snapshot all collections",
    )

    parser.add_argument(
        "--collection-name-filter",
        default=None,
        type=str,
        help="Filter for name of collections to include",
    )

    parser.add_argument(
        "--exclude-collection",
        default=[],
        nargs="*",
        help="Collection ids to exclude",
    )

    parser.add_argument(
        "--download-files",
        "-s",
        default=False,
        action="store_true",
        help="Download the files to disk.",
    )

    parser.add_argument(
        "--original-names",
        default=False,
        action="store_true",
        help="Use original filenames (without concatenating a uid). This will fail if two files in the collection have the same name",
    )

    parser.add_argument(
        "--labeled-files-only",
        default=False,
        action="store_true",
        help="Only saved labeled files",
    )

    parser.add_argument(
        "--filter-collection",
        default=None,
        type=str,
        help="Only snapshot files that are also in this collection uid",
    )

    parser.add_argument(
        "--label-filter",
        default=None,
        type=str,
        help="By default, snapshot only uses confirmed labels. This filter will treat any record that matches as confirmed",
    )

    parser.add_argument(
        "--allow-low-confidence",
        default=False,
        action="store_true",
        help="Allow low confidence predictions while snapshotting with a label filter",
    )

    parser.add_argument(
        "--field-mapping",
        default=None,
        type=str,
        help="A mapping of src_field:target_field names to change the field names while snapshotting",
    )

    parser.set_defaults(func=main)
    return parser


def download_file_to(url, path):
    r = requests.get(url)
    with open(path, "wb") as f:
        f.write(r.content)


def download_files(records, parallelism, workdir):
    with ThreadPoolExecutor(max_workers=parallelism) as t:
        [
            _
            for _ in t.map(
                lambda r: download_file_to(r["url"], workdir / r["name"]),
                records,
            )
        ]


def main(args):
    if (not args.collection and not args.all_collections) or (args.collection and args.all_collections):
        log.fatal("Must specify exactly one of --collection or --all-collections")
        exit(1)

    credentials = Credentials.load(**vars(args))
    if credentials is None:
        log.fatal("Unauthorized access")
        exit(1)

    impira = Impira(config=credentials)

    if args.all_collections:
        conn = impira._conn()
        name_filter = f"name:'{args.collection_name_filter}'" if args.collection_name_filter else ""
        collections = [
            r["uid"]
            for r in conn.query(f"@file_collections[uid] {name_filter}")["data"]
            if r["uid"] not in args.exclude_collection
        ]
    else:
        collections = [c for c in args.collection]

    workdir = pathlib.Path(args.data) / "capture" / f"{collections[0]}-{uuid4().hex[:4]}"

    field_mapping = None
    if args.field_mapping:
        mapping = [
            (a, b)
            for (a, b) in [tuple([a.strip() for a in x.strip().split(":", 1)]) for x in args.field_mapping.split(",")]
            if a and b
        ]
        field_mapping = dict(mapping)

    schema = DocSchema(fields={})
    records = []
    for collection_uid in collections:
        log.info(
            "Snapshotting collection %s"
            % (urljoin(impira.config.base_url, "o", impira.config.org_name, "fc", collection_uid))
        )
        collection_schema, collection_records = impira.snapshot(
            collection_uid=collection_uid,
            use_original_filenames=args.original_names,
            labeled_files_only=args.labeled_files_only,
            filter_collection_uid=args.filter_collection,
            label_filter=args.label_filter,
            allow_low_confidence=args.allow_low_confidence,
            field_mapping=field_mapping,
        )
        schema.fields.update(collection_schema.fields)
        records.extend(collection_records)

    log.info("Downloading %d files to %s", len(records), workdir)
    workdir.mkdir(parents=True, exist_ok=True)

    if args.download_files:
        download_files(records, args.parallelism, workdir)
        docs = [{"fname": r["name"], "record": r["record"]} for r in records]
    else:
        docs = [{"fname": r["name"], "url": r["url"], "record": r["record"]} for r in records]

    manifest = DocManifest(
        doc_schema=schema,
        docs=docs,
    )
    with open(workdir / "manifest.json", "w") as f:
        f.write(manifest.json(indent=2))

    # Print to stdout so we can pass it along in a script
    log.info("Documents and labels have been written to directory:")
    print(workdir)
