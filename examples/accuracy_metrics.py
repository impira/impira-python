#!/usr/bin/env python3

import argparse
from collections import defaultdict

import impira
from impira.tools.impira import filter_inferred_fields


def compute_metrics(data):
    # The pure straight through processing rate is defined as the
    # (high confidence predictions)/(number of values)
    #
    # If you assume the model is trained (i.e. you do not need to provide any more labels),
    # then you can compute the extrapolated straight through processing rate as
    # (high confidence predictions)/(number of values - number of user labels)
    #
    # The reported accuracy rate is the number of ratio of high confidence predictions:
    # (high confidence predictions)/(number of high confidence predictions + number of low confidence predictions)
    return {
        "Pure Straight Through Processing Rate": data["high"] / data["total"],
        "Extrapolated Straight Through Processing Rate": data["high"] / (data["total"] - data["labels"]),
        "Reported Accuracy Rate": data["high"] / (data["high"] + data["low"]),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--org-name", type=str, default=None, required=True)
    parser.add_argument("--api-token", type=str, default=None, required=True)
    parser.add_argument("--base-url", type=str, default="https://app.impira.com")
    parser.add_argument("--exclude", nargs="*", help="Field names to exclude")
    parser.add_argument("--limit", type=int, default=None, help="Only analyze this many fields")
    parser.add_argument("collection_name", type=str)

    args = parser.parse_args()

    conn = impira.Impira(org_name=args.org_name, api_token=args.api_token)
    collection_uid = conn.get_collection_uid(args.collection_name)

    schema = conn.query(f"@`file_collections::{collection_uid}` limit:0")["schema"]
    exclude_fields = set(args.exclude)
    inferred_fields = [x["name"] for x in filter_inferred_fields(schema["children"]) if x["name"] not in exclude_fields]
    if args.limit:
        inferred_fields = inferred_fields[: args.limit]

    print(f"Analyzing {len(inferred_fields)} inferred fields: ({inferred_fields})")

    # We want to compute, for each field:
    #   1) The number of values
    #   2) The number of user labels (black colored cells)
    #   3) The number of high confidence predictions (green colored cells)
    #   4) The number of low confidence predictions (orange colored cells)
    field_info = "\n, ".join(
        [
            f"""
    `{name}`: {{
        total:  count(),
        labels: sum(-`{name}`.Label.IsPrediction),
        high:   sum(`{name}`.Label.IsPrediction and `{name}`.Label.IsConfident),
        low:    sum(`{name}`.Label.IsPrediction and -`{name}`.Label.IsConfident),
    }}"""
            for name in inferred_fields
        ]
    )

    data = conn.query(f"@`file_collections::{collection_uid}`[{field_info}]")["data"][0]

    totals = defaultdict(lambda: 0)
    for field in inferred_fields:
        values = data[field]
        for k, v in values.items():
            totals[k] += v

        print(f"Analyzing field `{field}`")
        print(compute_metrics(values))

    print(f"Across all {len(inferred_fields)} fields")
    print(compute_metrics(totals))
