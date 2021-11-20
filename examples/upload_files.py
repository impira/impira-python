import argparse
import os
from urllib.parse import urlparse

import impira

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--org", type=str, default=None, required=True)
    parser.add_argument("--token", type=str, default=None, required=True)
    parser.add_argument("--collection", type=str, default=None, required=True)
    parser.add_argument(
        "files",
        metavar="file",
        type=str,
        nargs="+",
        help="Files to upload (either URLs or local files)",
    )

    args = parser.parse_args()

    impira_api = impira.Impira(org_name=args.org, api_token=args.token)

    collection_id = impira_api.get_collection_uid(args.collection)
    assert collection_id is not None

    files = [
        {"path": f, "name": os.path.split(urlparse(f).path)[1]} for f in args.files
    ]
    uids = impira_api.upload_files(collection_id, files)
    print(uids)

    for data in impira_api.poll_for_results(collection_id, uids):
        print(data)
