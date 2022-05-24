import argparse
import logging
import multiprocessing
import sys

from . import bootstrap
from . import capture
from . import snapshot
from . import snapshot_collections


def main(args=None):
    """The main routine."""
    if args is None:
        args = sys.argv[1:]

    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument("--verbose", "-v", default=False, action="store_true")
    parent_parser.add_argument("--parallelism", default=multiprocessing.cpu_count(), type=int)

    parser = argparse.ArgumentParser(description="impira is a CLI tool to work with Impira.")
    subparsers = parser.add_subparsers(help="sub-command help", dest="subcommand", required=True)

    for module in [capture, snapshot, bootstrap, snapshot_collections]:
        cmd_parser = module.build_parser(subparsers, parent_parser)

    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", level=level)

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
