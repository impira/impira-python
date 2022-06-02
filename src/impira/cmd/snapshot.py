from concurrent.futures import ThreadPoolExecutor
import pathlib
import requests
from shutil import copyfile
from uuid import uuid4

from ..tools.impira import Impira
from ..config import get_logger
from ..schema import record_to_schema
from ..types import DocManifest
from .utils import add_datadir_arg

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
        default=None,
        type=str,
        required=True,
        help="uid of the collection to snapshot",
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
    impira = Impira(config=Impira.Config(**vars(args)))
    workdir = pathlib.Path(args.data) / "capture" / f"{args.collection}-{uuid4().hex[:4]}"

    schema, records = impira.snapshot(
        collection_uid=args.collection,
        use_original_filenames=args.original_names,
        labeled_files_only=args.labeled_files_only,
    )

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

    log.info("Documents and labels have been written to directory '%s'", workdir)
