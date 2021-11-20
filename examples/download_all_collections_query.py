import argparse
import os
from urllib.parse import urlparse

import impira

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--org", type=str, default=None, required=True)
    parser.add_argument("--token", type=str, default=None, required=True)

    args = parser.parse_args()

    impira_api = impira.Impira(org_name=args.org, api_token=args.token)

    ecs = [
        x
        for x in impira_api.query(
            '@__system::ecs[name, fields: (fields[.: field.name] field_template="inferred_field_spec"), display_name: join_one(file_collections, name, field_ec).name] name:"file_collections::*"'
        )["data"]
    ]

    full_query = "\n\t".join(
        [
            "@`%s`[.: __resolve({Collection: '%s', uid, `File name`: File.name, %s})] limit:-1"
            % (
                x["name"],
                x["display_name"],
                ", ".join(["`%s`" % f for f in x["fields"]]),
            )
            for x in ecs
        ]
    )

    print(full_query)
    exit(0)
