import pathlib
from shutil import copyfile
from uuid import uuid4

from ..tools.textract import Textract
from ..config import get_logger
from ..schema import record_to_schema
from ..types import DocManifest
from .utils import add_datadir_arg

log = get_logger("infer-fields")


def build_parser(subparsers, parent_parser):
    capture = subparsers.add_parser(
        "infer-fields",
        help="process a document with AWS Textract and save its fields",
        parents=[parent_parser],
    )
    capture.add_argument("file_name", type=str, help="path to document to use")
    Textract.add_arguments(capture)
    add_datadir_arg(capture)

    capture.set_defaults(func=main)
    return capture


def save_record(file_name, record, data_dir):
    fpath = pathlib.Path(file_name)
    fpath_prefix = fpath.name.rsplit(".", 1)[0].replace(" ", "-")
    fpath_prefix = "".join(
        [c for c in fpath_prefix if c.isalpha() or c.isdigit() or c == "-"]
    ).rstrip()
    workdir = pathlib.Path(data_dir).joinpath(
        "capture", fpath_prefix + "-" + str(uuid4())[:4]
    )
    workdir.mkdir(parents=True, exist_ok=True)
    copyfile(file_name, workdir.joinpath(fpath.name))

    docs = [{"fname": fpath.name, "record": record}]
    manifest = DocManifest(doc_schema=record_to_schema(record), docs=docs)

    with open(workdir.joinpath("manifest.json"), "w") as f:
        f.write(manifest.json(indent=2))

    return workdir


def main(args):
    log.info("Running '%s' through textract", args.file_name)
    textract = Textract(config=Textract.Config(**vars(args)))
    record = textract.process_document(args.file_name)

    workdir = save_record(args.file_name, record, args.data)
    log.info("Document has been written to directory '%s'", workdir)
