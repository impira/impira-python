import os
import tempfile


def environ_or_required(key, default=None):
    return (
        {"default": os.environ.get(key, default)} if os.environ.get(key, default) is not None else {"required": True}
    )


# def credential_or_required(env_key, org_name=None, base_url=None):
#   if not CREDENTIALS_PATH.exists():


def add_datadir_arg(parser):
    parser.add_argument(
        "--data",
        "-d",
        help="Directory to save documents.",
        **environ_or_required("IMPIRA_DATA_DIR", os.path.join(tempfile.gettempdir(), "impira-cli")),
    )
