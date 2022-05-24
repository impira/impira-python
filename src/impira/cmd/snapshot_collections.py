from concurrent.futures import ThreadPoolExecutor
import pathlib
import requests
from shutil import copyfile
from uuid import uuid4

from ..tools.impira import Impira
from ..config import get_logger
from ..schema import record_to_schema
from ..types import DocSchema, DocManifest
from .utils import add_datadir_arg
from .snapshot import download_files

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
        "--original-names",
        default=False,
        action="store_true",
        help="Use original filenames (without concatenating a uid). This will fail if two files in the collection have the same name",
    )

    parser.set_defaults(func=main)
    return parser


def main(args):
    impira = Impira(config=Impira.Config(**vars(args)))
    workdir = pathlib.Path(args.data).joinpath("collections", str(uuid4())[:4])

    doc_schema, records = impira.snapshot_collections(
        use_original_filenames=args.original_names,
    )

    log.info("Downloading %d files to %s", len(records), workdir)
    workdir.mkdir(parents=True, exist_ok=True)

    if args.download_files:
        download_files(records, args.parallelism)
        docs = [{"fname": r["name"], "record": r["record"]} for r in records]
    else:
        docs = [{"fname": r["name"], "url": r["url"], "record": r["record"]} for r in records]

    manifest = DocManifest(doc_schema=doc_schema, docs=docs)
    with open(workdir.joinpath("manifest.json"), "w") as f:
        f.write(manifest.json(indent=2))

    log.info("Documents and collection labels have been written to directory '%s'", workdir)
