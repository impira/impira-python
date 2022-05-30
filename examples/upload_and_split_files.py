import argparse
import os
from time import sleep
from urllib.parse import urlparse

import impira

if __name__ == "__main__":
    # To provide an example of file modification via the Upload API, this simple script will upload the first 5 pages
    # of any local document or URL provided, and split the resulting document into individual pages.
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

    # For more information on supported file mutations, visit:
    # www.impira.com/documentation/file-modification-during-api-upload
    files = [
        {
            "path": f,
            "name": os.path.split(urlparse(f).path)[1],
            "mutate": {
                "remove_pages": "5:",
                "split": "page",
            },
        }
        for f in args.files
    ]
    # Note: upload_files can return a generator. We can fetch the string UIDs using list(...).
    uids = list(impira_api.upload_files(collection_id, files))
    print(uids)

    for data in impira_api.poll_for_results(collection_id, uids):
        print(data)
