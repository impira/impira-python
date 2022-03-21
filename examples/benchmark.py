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

START_CURSOR = "0200000000000000000001"

TABLE_TRAINER = "entity_one_many"
TEXT_TRAINER = "text_string-dev-1"

NUM_ITERS = 10


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
    api.query(query=query, mode="poll", cursor=START_CURSOR, timeout=0)


def _get_inferred_fields(api: impira.Impira, collection_id: str):
    query = """
    @`__system::ecs`[id, fields: (fields[id, name: field.name, template: field_template, trainer: infer_func.trainer_name])] name="file_collections::%s"
    [.:flatten(merge_unnest(build_entity("ec_id", id), fields))]
    [.] template="inferred_field_spec"
    """ % (collection_id)
    result = api.query(query)
    return result["data"]


def _get_field_info_with_trainer(api: impira.Impira, collection_id: str, trainer: str):
    inferred_fields = _get_inferred_fields(api, collection_id)
    ret = None
    for field in inferred_fields:
        if field["trainer"] == trainer:
            ret = field

    assert ret != None, "Could not find field with trainer: %s" % trainer
    return ret


def get_table_field_info(api: impira.Impira, collection_id: str):
    return _get_field_info_with_trainer(api, collection_id, TABLE_TRAINER)


def get_text_field_info(api: impira.Impira, collection_id: str):
    return _get_field_info_with_trainer(api, collection_id, TEXT_TRAINER)


def escape(input: str):
    return "`{}`".format(input)


def _prepare_train_input_field_query(api: impira.Impira, org_id: int, collection_id: str, trainer: str, gz: bool):
    field_info = _get_field_info_with_trainer(api, collection_id, trainer)
    storage_ec = "forge::{}::{}".format(org_id, field_info["ec_id"])
    storage_name = "f%d" % field_info["id"]
    field_name = field_info["name"]

    projections = ["uid", "file_id: File.file_id"]

    if gz:
        field_heap = "{}::fixed_schema_normalized::stitched::{}".format(
            storage_ec, storage_name)
        join = "{0}: join_one({1}, uid, uid).{0}".format(
            escape(field_name), field_heap)
        projections.append(join)
        projections.append("user_tag: {}".format(escape(field_name)))

    else:
        field_heap = "{}::fields::{}".format(storage_ec, storage_name)
        if trainer == TABLE_TRAINER:
            field_heap += "::stitched"

        join = "{0}: join_one({1}, uid, uid).{0}[raw_user_tag_v2]".format(
            storage_name, field_heap)

        projections.append(join)
        projections.append("user_tag: {}.raw_user_tag_v2".format(storage_name))

    assert field_heap != None

    query = "@`{}::data`[{}]".format(storage_ec, ", ".join(projections))

    return [api, query]


def train_input_text_field_query(api: impira.Impira, query: str):
    api.query(query=query, mode="poll", cursor=START_CURSOR, timeout=0)


def benchmark_function(function, prepare_fn, num_runs, args):
    total_time = 0
    for _ in range(num_runs):
        # Prepare arguments before timing the function
        arguments = args
        if prepare_fn != None:
            arguments = prepare_fn(*args)

        start = time.time()
        function(*arguments)
        end = time.time()

        delta = end - start
        total_time += (delta * 1000)

    return round(total_time / num_runs, 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--org-name", type=str, default=None, required=True)
    parser.add_argument("--org-id", type=int, default=None, required=True)
    parser.add_argument("--api-token", type=str, default=None, required=True)
    parser.add_argument("--collection", type=str, default=None)
    parser.add_argument("--base-url", type=str, default=None, required=True)
    parser.add_argument("--gz", default=False, action="store_true")
    parser.add_argument(
        "--parallelism", default=multiprocessing.cpu_count(), type=int)

    parser.add_argument(
        "--data",
        "-d",
        type=str,
        default=None,
        help="Directory containing data to bootstrap collection. This directory should contain one or more documents and a manifest (see `impira snapshot -h`). These documents should already be uploaded to the organization.",
    )

    args = parser.parse_args()

    impira_api = impira.Impira(
        org_name=args.org_name, api_token=args.api_token, base_url=args.base_url)

  # Create the collection if not specified by user
    collection_id = args.collection
    if args.collection is None:
        assert args.data != None, "--data required to bootstrap collection"
        name = "Benchmark-" + str(time.time_ns() // 1000)
        collection_id = impira_api.create_collection(name)
        print("Created collection '%s' with uid='%s'" % (name, collection_id))

    # Bootstrap the collection

    if args.data != None:
        manifest = prepare_manifest(args.data)

        print("If this is the first time adding these files to this collection, the setup will probably hang. Once evaluation has concluded, you should CTRL+C and restart this benchmark.")

        bootstrap = ImpiraBootstrap(
            config=ImpiraBootstrap.Config(**vars(args)))
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
        print("Setup collection with uid: %s" % (collection_id))

    else:
        assert args.collection != None, "--collection required if not bootstrapping"

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
        {
            "name": "Train Input Text Field Query",
            "prepare_fn": _prepare_train_input_field_query,
            "function": train_input_text_field_query,
            "args": [impira_api, args.org_id, collection_id, TEXT_TRAINER, args.gz]
        },
        {
            "name": "Train Input Table Field Query",
            "prepare_fn": _prepare_train_input_field_query,
            "function": train_input_text_field_query,
            "args": [impira_api, args.org_id, collection_id, TABLE_TRAINER, args.gz]
        }
    ]

    for benchmark in benchmarks:
        avg_time = benchmark_function(benchmark["function"], benchmark.get(
            "prepare_fn", None), NUM_ITERS, benchmark["args"])
        print("{}: {}ms".format(benchmark["name"], avg_time))
