from getpass import getpass
import pathlib
from concurrent.futures import ThreadPoolExecutor
import sys
from uuid import uuid4

import requests

from ..api_v2 import InvalidRequest, urljoin
from ..config import get_logger
from ..tools.impira import Impira
from ..types import DocManifest, DocSchema
from .utils import add_datadir_arg
from ..credentials import Credentials, CREDENTIALS_PATH, get_credentials, save_credentials


log = get_logger("login")


def build_parser(subparsers, parent_parser):
    parser = subparsers.add_parser(
        "login",
        help="Log into an Impira Organization",
        parents=[parent_parser],
    )

    parser.add_argument(
        "--org-name",
        default=None,
        type=str,
        help="Log into a specific organization",
    )

    parser.add_argument(
        "--base-url",
        default="https://app.impira.com",
        type=str,
        help="Log into a specific deployment",
    )

    parser.set_defaults(func=main)
    return parser


ATTEMPTS = 10


def _login(org_name=None, base_url=None):
    print("If you have not already created an API token in Impira, please follow the instructions here:")
    print("  https://docs.impira.com/ref#how-to-create-an-api-token")

    if org_name is None:
        print("Org Name (the part after /o/ in your Impira URL): ", end="")
        sys.stdout.flush()
        org_name = input()
    token_url = urljoin(base_url, "o", org_name, "access", "token_management")
    print(f"You can find your API token at {token_url}")
    print("API Token (click the reveal button to copy it): ", end="")
    sys.stdout.flush()
    api_token = getpass("")
    return Credentials(api_token=api_token, org_name=org_name, base_url=base_url)


def main(args):
    credentials = get_credentials(org_name=args.org_name, base_url=args.base_url)
    should_save = False

    for i in range(ATTEMPTS):
        if credentials is not None:
            try:
                impira = Impira(config=credentials)
                impira._conn()
                log.info(f"Successfully logged into {impira.config.base_url}/o/{impira.config.org_name}")
                break
            except InvalidRequest:
                pass

        if i > 0:
            log.warning("Failed to login with provided credentials. Let's try again")

        credentials = _login(args.org_name, args.base_url)
        should_save = True
    else:
        log.fatal(f"Unable to login after {ATTEMPTS} attempts. Please try again later")
        exit(1)

    if should_save:
        log.info(f"Successfully saved credentials to {CREDENTIALS_PATH}.")
        save_credentials(credentials)
