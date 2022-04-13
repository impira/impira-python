#!/usr/bin/env python3

import argparse
import boto3
import impira
from impira.utils import batch
import logging
import multiprocessing
import pathlib
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse


def entry_to_uid(bucket, key):
    return "s3://%s/%s" % (bucket, key)


def files_exist(conn, bucket, keys):
    uids = [entry_to_uid(bucket, key) for key in keys]
    uploaded = conn.query("@files[uid] in(uid, %s)" % (",".join(["'%s'" % (uid) for uid in uids])))["data"]
    uploaded_set = set([x["uid"] for x in uploaded])
    return [uid in uploaded_set for uid in uids]


def upload_batch(args):
    conn, s3, idx, collection_id, bucket, entries = args
    keys = [x["Key"] for x in entries]
    existing = files_exist(conn, bucket, keys)
    new = [x for (i, x) in enumerate(keys) if not existing[i]]
    logging.info("Batch %d loading %d/%d files" % (idx, len(new), len(entries)))
    conn.upload_files(
        collection_id,
        [
            {
                "uid": entry_to_uid(bucket, key),
                "name": pathlib.Path(key).name,
                "path": s3.generate_presigned_url(
                    "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=3600 * 24 * 7
                ),
            }
            for key in new
        ],
    )
    logging.info("Batch %d loaded %d files!" % (idx, len(new)))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--org-name", type=str, default=None, required=True)
    parser.add_argument("--api-token", type=str, default=None, required=True)
    parser.add_argument("--collection-name", type=str, default=None)
    parser.add_argument("--force", action="store_true", help="Upload files, even if they have already been loaded")
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("s3_path", type=str)

    args = parser.parse_args()
    conn = impira.Impira(org_name=args.org_name, api_token=args.api_token)

    collection_id = None
    if args.collection_name is not None:
        collections = conn.query('@file_collections[uid] name="%s"' % (args.src_collection_name))["data"]
        if len(collections) == 0:
            logging.fatal("Could not find collection named '%s'", args.src_collection_name)
            exit(1)
        if len(collections) > 1:
            logging.fatal("Multiple collections named '%s'", args.src_collection_name)
            exit(1)

        collection_id = collections[0].uid

    url = urlparse(args.s3_path)

    if url.scheme != "s3":
        logging.fatal("Can only load files from s3, not %s", url.scheme)
        exit(1)

    bucket = url.netloc
    prefix = url.path.lstrip("/")

    s3 = boto3.client("s3")
    files = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)["Contents"]
    file_batches = [x for x in batch(files, args.batch_size)]

    logging.info("Found %d files in %d batches", len(files), len(file_batches))

    executor = ThreadPoolExecutor(max_workers=4 * multiprocessing.cpu_count())
    [
        x
        for x in executor.map(
            upload_batch, [(conn, s3, i, collection_id, bucket, entries) for (i, entries) in enumerate(file_batches)]
        )
    ]
    logging.info("Done!")
