from enum import Enum
import json
import os
from pydantic import BaseModel, Field, validate_arguments
import requests
from urllib.parse import quote_plus
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse


class FilePath(BaseModel):
    name: str
    path: str
    uid: Optional[str]


class FieldType(str, Enum):
    string = "STRING"
    number = "NUMBER"
    bool = "BOOL"
    timestamp = "TIMESTAMP"
    entity = "ENTITY"


class InferredFieldType(Enum):
    text = {"expression": "`text_string-dev-1`(File.text)", "type": "STRING"}
    number = {"expression": "`text_number-dev-1`(File.text)", "type": "NUMBER"}
    timestamp = {"expression": "`text_date-dev-1`(File.text)", "type": "TIMESTAMP"}
    table = {
        "expression": "entity_one_many(File.text)",
        "type": "ENTITY",
        "isList": True,
    }


class FieldSpec(BaseModel):
    field: str  # This is the field's name
    type: FieldType
    expression: Optional[str]
    path: Optional[List[str]]
    isList: Optional[bool]


class InvalidRequest(Exception):
    pass


class APIError(Exception):
    def __init__(self, response):
        self.response = response

    def __str__(self):
        return "%s: %s" % (self.response.status_code, self.response.content)


class IQLError(Exception):
    pass


class ResourceType(str, Enum):
    fc = "fc"
    dc = "dc"
    collection = "collection"


class Impira:
    def __init__(
        self, org_name, api_token, base_url="https://app.impira.com", ping=True
    ):
        self.org_url = os.path.join(base_url, "o", org_name)
        self.api_url = os.path.join(self.org_url, "api/v2")
        self.headers = {"X-Access-Token": api_token}

        if ping:
            try:
                self._ping()
            except Exception as e:
                raise InvalidRequest(
                    "Failed to ping Impira API. Please check your org name and API token. Full error: %s"
                    % str(e)
                )

    @validate_arguments
    def upload_files(self, collection_id: str, files: List[FilePath]):
        local_files = len([f for f in files if urlparse(f.path).scheme in ("", "file")])
        if local_files > 0 and local_files != len(files):
            raise InvalidRequest(
                "All files must be local or URLs, but not a mix (%d/%d were local)"
                % (local_files, len(files))
            )
        elif local_files > 0:
            return self._upload_multipart(collection_id, files)
        else:
            return self._upload_url(collection_id, files)

    @validate_arguments
    def get_collection_uid(self, collection_name: str):
        resp = self.query('@__system::collections[uid] Name="%s"' % (collection_name))[
            "data"
        ]
        if len(resp) == 0:
            return None

        uids = [x["uid"] for x in resp]
        if len(uids) > 1:
            raise InvalidRequest(
                "Found multiple collections with name '%s': %s"
                % (collection_name, ", ".join(uids))
            )

        return uids[0]

    @validate_arguments
    def update(self, collection_id: str, data: List[Dict[str, Any]]):
        resp = requests.patch(
            self._build_resource_url("fc", collection_id),
            headers=self.headers,
            json={"data": data},
        )

        if not resp.ok:
            raise APIError(resp)

        return resp.json()["uids"]

    @validate_arguments
    def get_app_url(self, resource_type: ResourceType, resource_id: str) -> str:
        return self._build_resource_url(
            resource_type, resource_id, api=False, use_async=False
        )

    @validate_arguments
    def create_collection(self, collection_name: str):
        existing = self.get_collection_uid(collection_name)
        if existing is not None:
            raise InvalidRequest(
                "Collection with name '%s' already exists at %s"
                % (collection_name, os.path.join(self.org_url, "fc", existing))
            )

        # Create collection is implemented as an empty insert
        resp = requests.post(
            self._build_resource_url("collection", collection_name),
            headers=self.headers,
            json={"data": []},
        )

        if not resp.ok:
            raise APIError(resp)

        uid_list = resp.json()["uids"]
        assert (
            not uid_list
        ), "Expected empty uid list while creating a collection, but received: %s" % (
            ", ".join(uid_list)
        )

        return self.get_collection_uid(collection_name)

    @validate_arguments
    def create_field(self, collection_id: str, field_spec: FieldSpec):
        resp = requests.post(
            os.path.join(
                self.api_url, "schema/ecs/file_collections::%s/fields" % (collection_id)
            ),
            headers=self.headers,
            json=dict(field_spec),
        )

        if not resp.ok:
            raise APIError(resp)

    @validate_arguments
    def create_inferred_field(
        self,
        collection_id: str,
        field_name: str,
        inferred_field_type: InferredFieldType,
        path: List[str] = [],
    ):
        field_spec = FieldSpec(field=field_name, path=path, **inferred_field_type.value)
        return self.create_field(collection_id, field_spec)

    @validate_arguments
    def poll_for_results(self, collection_id: str, uids: List[str] = None):
        uid_filter = (
            "and in(uid, %s)" % (", ".join(['"%s"' % u for u in uids])) if uids else ""
        )
        query = """
        @`file_collections::%s`
            File.IsPreprocessed=true and __system.IsProcessed=true
            %s
            [.: __resolve(.)]""" % (
            collection_id,
            uid_filter,
        )

        cursor = None

        must_see = set(uids)
        while True:
            resp = self.query(query, mode="poll", cursor=cursor, timeout=60)
            for d in resp["data"] or []:
                if d["action"] != "insert":
                    continue
                yield d["data"]

                uid = d["data"]["uid"]
                if len(must_see) > 0:
                    assert uid in must_see, "Broken uid filter (%s not in %s)" % (
                        uid,
                        must_see,
                    )
                    must_see.remove(uid)

            cursor = resp["cursor"]

            if len(must_see) == 0:
                break

    @validate_arguments
    def query(
        self, query: str, mode: str = "iql", cursor: str = None, timeout: str = None
    ):
        args = {"query": query}
        if cursor is not None:
            args["cursor"] = cursor

        if timeout is not None:
            args["timeout"] = timeout

        resp = requests.post(
            os.path.join(self.api_url, mode),
            headers=self.headers,
            json={"query": query},
        )
        if not resp.ok:
            raise APIError(resp)

        d = resp.json()
        if "error" in d and d["error"] is not None:
            raise IQLError(d["error"])

        return resp.json()

    def _ping(self):
        self.query("@files limit:0")

    @validate_arguments
    def _upload_multipart(self, collection_id: str, files: List[FilePath]):
        for f in files:
            if f.uid is not None:
                raise InvalidRequest(
                    "Unsupported: specifying a UID in a multi-part file upload (%s)"
                    % (f.uid)
                )

        files_body = [
            t
            for f in files
            for t in [
                ("file", open(f.path, "rb")),
                ("data", json.dumps(_build_file_object(f.name, None, f.uid))),
            ]
        ]
        resp = requests.post(
            self._build_collection_url(collection_id, use_async=True),
            headers=self.headers,
            files=tuple(files_body),
        )
        if not resp.ok:
            raise APIError(resp)

        return resp.json()["uids"]

    @validate_arguments
    def _upload_url(self, collection_id: str, files: List[FilePath]):
        resp = requests.post(
            self._build_collection_url(collection_id, use_async=True),
            headers=self.headers,
            json={"data": [_build_file_object(f.name, f.path, f.uid) for f in files]},
        )
        if not resp.ok:
            raise APIError(resp)

        return resp.json()["uids"]

    @validate_arguments
    def _upload_url_collection_name(self, collection_name: str, files: List[FilePath]):
        resp = requests.post(
            self._build_collection_url(collection_id, use_async=True),
            headers=self.headers,
            json={"data": [_build_file_object(f.name, f.path, f.uid) for f in files]},
        )
        if not resp.ok:
            raise APIError(resp)

        return resp.json()["uids"]

    @validate_arguments
    def _build_collection_url(self, collection_id: str, use_async=False):
        return self._build_resource_url("fc", collection_id, use_async)

    @validate_arguments
    def _build_resource_url(
        self, resource_type: ResourceType, resource_id: str, api=True, use_async=False
    ):
        base_url = os.path.join(
            self.api_url if api else self.org_url,
            resource_type,
            quote_plus(resource_id),
        )
        if use_async:
            base_url = base_url + "?async=1"
        return base_url


def _build_file_object(name, path=None, uid=None):
    ret = {"File": {"name": name}}
    if path is not None:
        ret["File"]["path"] = path
    if uid is not None:
        ret["uid"] = uid
    return ret
