import pathlib
from concurrent.futures import ThreadPoolExecutor
from shutil import copyfile
from uuid import uuid4

import requests

from ..config import get_logger
from ..schema import record_to_schema
from ..tools.impira import Impira
from ..types import DocManifest, DocSchema
from .snapshot import download_files
from .utils import add_datadir_arg
from ..credentials import Credentials


log = get_logger("snapshot-collections")


def build_parser(subparsers, parent_parser):
    parser = subparsers.add_parser(
        "snapshot-collections",
        help="Snapshot the files and which collection(s) they belong to in an Impira org",
        parents=[parent_parser],
    )
    Impira.add_arguments(parser)

    add_datadir_arg(parser)

    parser.add_argument(
        "--download-files",
        "-s",
        default=False,
        action="store_true",
        help="Download the files to disk.",
    )

    parser.add_argument(
        "--max-files-per-collection",
        type=int,
        default=50,
        help="The maximum number of files per collection to snapshot. Set to -1 to snapshot all of them.",
    )

    parser.add_argument(
        "--original-names",
        default=False,
        action="store_true",
        help="Use original filenames (without concatenating a uid). This will fail if two"
        " files in the collection have the same name",
    )

    parser.add_argument(
        "--samples-per-collection",
        type=int,
        default=2,
        help="The number of confirmed document tag labels that get created from each collection.",
    )

    parser.add_argument(
        "--collection",
        default=[],
        nargs="*",
        help="Optional uid of one or more collections to snapshot",
    )

    parser.set_defaults(func=main)
    return parser


def main(args):
    credentials = Credentials.load(**vars(args))
    if credentials is None:
        log.fatal("Unauthorized access")
        exit(1)

    impira = Impira(config=credentials)
    workdir = pathlib.Path(args.data) / "collections" / uuid4().hex[:4]

    doc_schema, records = impira.snapshot_collections(
        use_original_filenames=args.original_names,
        max_files_per_collection=args.max_files_per_collection,
        num_samples=args.samples_per_collection,
        collection_filter=args.collection if args.collection else None,
    )

    log.info("Downloading %d files to %s", len(records), workdir)
    workdir.mkdir(parents=True, exist_ok=True)

    if args.download_files:
        download_files(records, args.parallelism, workdir)
        docs = [{"fname": r["name"], "record": r["record"]} for r in records]
    else:
        docs = [{"fname": r["name"], "url": r["url"], "record": r["record"]} for r in records]

    manifest = DocManifest(doc_schema=doc_schema, docs=docs)
    with open(workdir / "manifest.json", "w") as f:
        f.write(manifest.json(indent=2))

    log.info("Documents and collection labels have been written to directory '%s'", workdir)
