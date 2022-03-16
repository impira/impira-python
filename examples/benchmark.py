import argparse
from datetime import timedelta
import multiprocessing
import os
import random
from urllib.parse import urlparse
import time

import impira

from impira.cmd.bootstrap import prepare_manifest

from impira.tools.impira import Impira as ImpiraBootstrap

BATCH_SIZE = 20
SEED = 333

ZERO_CURSOR = "0200000000000000000000"


def pagination_query(api: impira.Impira, collection_id: str):
    query = "@`file_collections::%s`[uid]" % (collection_id)
    return api.query(query)


def collection_query(api: impira.Impira, collection_id: str):
    query = "@`file_collections::%s`[.: __resolve(.)]" % (collection_id)
    return api.query(query)


def _preprare_uid_filtered_collection_query(api: impira.Impira, collection_id: str):
    result = pagination_query(api, collection_id)
    uids = []
    for row in result["data"]:
        uid = row.get("uid", None)
        if uid != None:
            uids.append(uid)

    assert len(uids) > 0, "Pagination query returned no uids"

    # Sort the UIDs and then shuffle using a seed to get deterministic filter for the benchmark across iterations
    uids.sort()
    random.Random(SEED).shuffle(uids)

    filters = []
    for uid in uids[:BATCH_SIZE]:
        filters.append("uid=\"{}\"".format(uid))

    query = "@`file_collections::%s`[.: __resolve(.)] %s" % (
        collection_id, " OR ".join(filters))
    return [api, query]


def uid_filtered_collection_query(api: impira.Impira, query: str):
    return api.query(query)


def ie_collection_query(api: impira.Impira, collection_id: str):
    query = "@`file_collections::%s`[.: __resolve(.)]" % (collection_id)
    api.query(query=query, mode="poll", cursor=ZERO_CURSOR, timeout=0)


# def _get_inferred_fields(api: impira.Impira, collection_id: str):
#     query = """
#     @`__system::ecs`[id, fields: (fields[id, name: field.name, template: field_template, trainer: infer_func.trainer_name])] name="file_collections::%s"
#     [.:flatten(merge_unnest(build_entity("ec_id", id), fields))]
#     [.] template="inferred_field_spec"
#     """ % (collection_id)
#     result = api.query(query)
#     return result


# def _get_field_info_with_trainer(api: impira.Impira, collection_id: str, trainer: str):
#     inferred_fields = _get_inferred_fields(api, collection_id)
#     ret = None
#     for field in inferred_fields:
#         if field["trainer"] == trainer:
#             ret = field

#     assert ret != None, "Could not find field with trainer: %s" % trainer
#     return ret

# def get_table_field_info(api: impira.Impira, collection_id: str):
#     return _get_field_info_with_trainer(api, collection_id, "text_number-dev-1")


# def get_text_field_info(api: impira.Impira, collection_id: str):
#     return _get_field_info_with_trainer(api, collection_id, "entity_one_many")


def benchmark_function(function, prepare_fn, num_runs, args):
    total_time = 0
    for _ in range(num_runs):
        start = time.time()
        arguments = args
        if prepare_fn != None:
            arguments = prepare_fn(*args)
        function(*arguments)
        end = time.time()
        delta = end - start
        total_time += (delta * 1000)

    return round(total_time / num_runs, 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--org-name", type=str, default=None, required=True)
    parser.add_argument("--api-token", type=str, default=None, required=True)
    parser.add_argument("--collection", type=str, default=None)
    parser.add_argument("--base-url", type=str, default=None, required=True)
    parser.add_argument(
        "--parallelism", default=multiprocessing.cpu_count(), type=int)

    parser.add_argument(
        "--data",
        "-d",
        required=True,
        type=str,
        help="Directory containing data to bootstrap collection. This directory should contain one or more documents and a manifest (see `impira snapshot -h`). These documents should already be uploaded to the organization.",
    )

    args = parser.parse_args()

    impira_api = impira.Impira(
        org_name=args.org_name, api_token=args.api_token, base_url=args.base_url)

    # Create the collection if not specified by user
    collection_id = args.collection
    if args.collection is None:
        name = "Benchmark-" + str(time.time_ns() // 1000)
        collection_id = impira_api.create_collection(name)
        print("Created collection '%s' with uid='%s'" % (name, collection_id))

    # Bootstrap the collection

    manifest = prepare_manifest(args.data)

    print("If this is the first time adding these files to this collection, the setup will probably hang. Once evaluation has concluded, you should CTRL+C and restart this benchmark.")

    bootstrap = ImpiraBootstrap(config=ImpiraBootstrap.Config(**vars(args)))
    bootstrap.run(
        manifest.doc_schema,
        manifest.docs,
        collection_prefix="impira-cli-benchmark",
        parallelism=args.parallelism,
        existing_collection_uid=collection_id,
        skip_type_inference=False,
        skip_upload=True,
        add_files=True,
        skip_new_fields=False,
    )

    # get_inferred_fields(impira_api, collection_id)

    print("Setup collection with uid: %s" % (collection_id))

    benchmarks = [
        {
            "name": "Pagination Query",
            "function": pagination_query,
            "args": [impira_api, collection_id]
        },
        {
            "name": "Full Collection Query",
            "function": collection_query,
            "args": [impira_api, collection_id]
        },
        {
            "name": "Uid Filtered Collection Query",
            "prepare_fn": _preprare_uid_filtered_collection_query,
            "function": uid_filtered_collection_query,
            "args": [impira_api, collection_id]
        },
        {
            "name": "Full Collection Query (IE)",
            "function": ie_collection_query,
            "args": [impira_api, collection_id]
        },
    ]

    num_runs = 5

    for benchmark in benchmarks:
        avg_time = benchmark_function(benchmark["function"], benchmark.get(
            "prepare_fn", None), num_runs, benchmark["args"])
        print("{}: {}ms".format(benchmark["name"], avg_time))
