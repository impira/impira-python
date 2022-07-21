"""
api_v2.py
=========

The core SDK module
"""

from datetime import datetime
from dateutil.parser import parse as dateparse
from enum import Enum
from http import HTTPStatus
import json
import logging
import math
from pydantic import BaseModel, Field, validate_arguments
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time
from typing import Any, Generator, Dict, List, Optional
from urllib.parse import quote_plus, urlparse


class RotateSegment(BaseModel):
    """A single segment (set of pages) to rotate."""

    pages: str
    degrees: int


class Mutation(BaseModel):
    """A class that allows you to configure the mutations to apply to a file.
    See the [Mutation API docs](https://docs.impira.com/ref#file-modification-during-api-upload) for
    more details on the available options.
    """

    rotate: Optional[int]
    split: Optional[str]
    remove_pages: Optional[str]
    split_segments: Optional[List[str]]
    rotate_segments: Optional[List[RotateSegment]]
    split_exprs: Optional[Dict[str, str]]

    def to_json(self):
        return self.dict(exclude_unset=True)


class FilePath(BaseModel):
    """A local or remote file that can be uploaded to Impira.

    :param name: The name of the file. This will appear in the Impira UI. The file's name does not have to be unique.
    :type name: str
    :param path: A path which is a local file or a remote file that Impira can access. The path is optionally URL-formatted. You can specify `file://` as the protocol for a local file, or use a remote protocol like `http://` for a remote file.
    :type path: str
    :param uid: The unique id for the file in Impira. Impira will automatically assign a unique `uid` if you do not specify one. If you do specify a `uid`, and the file already exists, the upload will overwrite the existing file as a new version.
    :type uid: str, optional
    :param mutate: A set of [mutations](https://docs.impira.com/ref#file-modification-during-api-upload) to apply while uploading the file.
    :type mutate: :py:class:\`Mutation\`, optional
    """

    name: str
    path: str
    uid: Optional[str]
    mutate: Optional[Mutation]


class FieldType(str, Enum):
    """An enumeration of various field types."""

    text = "STRING"
    number = "NUMBER"
    bool = "BOOL"
    timestamp = "TIMESTAMP"
    entity = "ENTITY"


class FieldSpec(BaseModel):
    """The definition of a field."""

    field: str  # This is the field's name
    type: FieldType
    expression: Optional[str]
    path: Optional[List[str]]
    isList: Optional[bool]


class InferredFieldType(Enum):
    """An enum that wraps inferred field types and contains helper methods to create
    :py:class:\`FieldSpec\` instances from them.
    """

    text = {"expression": "`text_string-dev-1`(File.text)", "type": "STRING"}
    number = {"expression": "`text_number-dev-1`(File.text)", "type": "NUMBER"}
    timestamp = {"expression": "`text_date-dev-1`(File.text)", "type": "TIMESTAMP"}
    checkbox = {"expression": "checkbox(File.text)", "type": "STRING"}
    signature = {"expression": "region_signature(File.text)", "type": "STRING"}
    table = {
        "expression": "entity_one_many(File.text)",
        "type": "ENTITY",
        "isList": True,
    }
    document_tag = {"expression": "document_tag(File)", "type": "ENTITY", "isList": True}

    @property
    def expr(self):
        return self.value["expression"]

    @classmethod
    def match_trainer(cls, trainer: str):
        ret = None
        for x in cls:
            if trainer in x.expr:
                assert ret is None, "Matched multiple trainers: %s and %s" % (ret, x)
                ret = x
        assert ret is not None, "Unknown trainer: %s" % (trainer)
        return ret

    @validate_arguments
    def build_field_spec(self, field_name: str, path: List[str] = []) -> FieldSpec:
        """Build a field spec for an inferred field type.

        :param field_name: The name of the field to create.
        :type field_name: str
        :param path: Specify a path if this is a sub-field of a table. For example, if this field should be created inside of a table named `T`, path should be `["T"]`.
        :type path: List[str]

        :returns A :py:class:\`FieldSpec\`."""

        return FieldSpec(field=field_name, path=path, **self.value)


class InvalidRequest(Exception):
    pass


class APIError(Exception):
    """An exception that wraps a failed response. The `response` field gives you access to the underlying response."""

    def __init__(self, response):
        self.response = response

    def __str__(self):
        return "%s: %s" % (self.response.status_code, self.response.content)


class IQLError(Exception):
    """An exception that wraps an invalid IQL query."""

    pass


class ResourceType(str, Enum):
    """An enumeration of the various resource types in Impira to help with generating URLs.

    :param fc: Reference a collection by its id.
    :param dc: Reference a dataset by its id.
    :param ec: Reference an entity class by its name (e.g. `file_collections::574764db867afdb9`).
    :param collection: Reference a collection by name.
    :param files: Reference "All files" (the global endpoint).

    """

    fc = "fc"
    dc = "dc"
    ec = "ec"
    collection = "collection"
    files = "files"


FMTS = ["%Y-%m-%d", "%Y-%m", "%Y"]


@validate_arguments
def parse_date(s: str) -> datetime:
    for i, fmt in enumerate(FMTS):
        try:
            return datetime.strptime(s, fmt)
        except ValueError as e:
            # From https://stackoverflow.com/questions/5045210/how-to-remove-unconverted-data-from-a-python-datetime-object
            if len(e.args) > 0 and e.args[0].startswith("unconverted data remains: "):
                prefix = s[: -(len(e.args[0]) - 26)]
                return datetime.strptime(prefix, fmt)

    # As a last ditch effort, try using dateparse
    return dateparse(s)


class Impira:
    """This class is the main wrapper around a connection to an Impira org (including
    credentials). It uses a `requests.Session` object to optimize usage across requests.
    You should assume this class is *not* threadsafe.

    :param org_name: Your org name. You can find this by logging into Impira and pulling out the text after `/o/` in your URL: `.../o/<YOUR_ORG_NAME>/...`
    :type org_name: str
    :param api_token: Your API token. See the [API docs](https://docs.impira.com/ref#how-to-create-an-api-token) for instructions on how to obtain it.
    :type api_token: str
    :param ping: By default, Impira will try to ping the API when you create a connection to verify your credentials. You can set this flag to `False` to disable this check.
    :type ping: bool
    """

    def __init__(self, org_name, api_token, base_url="https://app.impira.com", ping=True):
        """Constructor"""

        self.org_url = urljoin(base_url, "o", org_name)
        self.api_url = urljoin(self.org_url, "api/v2")
        self.session = requests.Session()
        self.session.headers.update({"X-Access-Token": api_token})

        # Following a suggestion in https://stackoverflow.com/questions/23013220/max-retries-exceeded-with-url-in-requests
        retry = Retry(connect=10, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        if ping:
            try:
                self._ping()
            except Exception as e:
                raise InvalidRequest(
                    "Failed to ping Impira API. Please check your org name and API token. Full error: %s" % str(e)
                )

    @validate_arguments
    def upload_files(self, collection_id: Optional[str], files: List[FilePath]):
        """The main entry point to upload files. Based on the paths of the files, this will
        automatically call the standard URL-based file upload (higher performance) or upload
        the files directly through the multi-part form API.

        :param collection_id: The collection to upload the files into. Specify `None` to upload the files to "All files".
        :type collection_id: str, optional
        :param files: A list of files (including their name, path, and any mutation options) to upload. These can be URLs or local files.
        :type files: List[:py:class:\`FilePath\`]

        :return: The uids of the uploaded files. If you specify mutations which result in a dynamic number of documents (e.g. page splitting), then the returned value will be a generator.
        """

        local_files = len([f for f in files if urlparse(f.path).scheme in ("", "file")])
        if local_files > 0 and local_files != len(files):
            raise InvalidRequest(
                "All files must be local or URLs, but not a mix (%d/%d were local)" % (local_files, len(files))
            )
        elif local_files > 0:
            return self._upload_multipart(collection_id, files)
        else:
            return self._upload_url(collection_id, files)

    @validate_arguments
    def get_collection_id(self, collection_name: str):
        """Retrieve the collection id corresponding to a given name.

        :param collection_name: The name of the collection to look up.
        :type collection_name: str

        :return: The uid of the collection.
        """

        resp = self.query('@__system::collections[uid] Name="%s"' % (collection_name))["data"]
        if len(resp) == 0:
            return None

        uids = [x["uid"] for x in resp]
        if len(uids) > 1:
            raise InvalidRequest("Found multiple collections with name '%s': %s" % (collection_name, ", ".join(uids)))

        return uids[0]

    @validate_arguments
    def update(self, collection_id: str, data: List[Dict[str, Any]]):
        """Update fields in a collection.

        :param collection_id: The collection to update.
        :type collection_id: str
        :param data: A list of data updates to perform. Each record should contain a `uid` field corresponding to the file to update.
        :type data: List[Dict[str, Any]]

        :return: The updated uids.
        """

        resp = self.session.patch(
            self._build_resource_url("fc", collection_id),
            params={"assert_updated": False},
            json={"data": data},
        )

        if not resp.ok:
            raise APIError(resp)

        return resp.json()["uids"]

    @validate_arguments
    def set_table_cell(self, collection_id: str, uid: str, table_name: str, path: List[str], data):
        # TODO: Document this function. It's fairly low-level as written, so we may want to wrap it before documenting it.
        return self._set_field_path("file_collections::" + collection_id, uid, [table_name] + path, data)

    @validate_arguments
    def add_files_to_collection(self, collection_id: str, file_ids: List[str]):
        """Add existing files to a collection.

        :param collection_id: The collection id to add the files into.
        :type collection_id: str
        :param file_ids: A list of file uids to add into the collection.
        :type file_ids: List[str]

        :return: None
        """

        resp = self.session.post(
            self._build_resource_url("ec", "file_collection_contents"),
            json={"data": [{"file_uid": u, "collection_uid": collection_id} for u in file_ids]},
        )

        if not resp.ok:
            raise APIError(resp)

    @validate_arguments
    def create_collection(self, collection_name: str):
        """Create a collection with the provided name.

        :param collection_name: The name of the collection to create.
        :type collection_name: str

        :return: The collection id of the newly created collection.
        """

        existing = self.get_collection_uid(collection_name)
        if existing is not None:
            raise InvalidRequest(
                "Collection with name '%s' already exists at %s"
                % (collection_name, urljoin(self.org_url, "fc", existing))
            )

        # Create collection is implemented as an empty insert
        resp = self.session.post(
            self._build_resource_url("collection", collection_name),
            json={"data": []},
        )

        if not resp.ok:
            raise APIError(resp)

        uid_list = resp.json()["uids"]
        assert not uid_list, "Expected empty uid list while creating a collection, but received: %s" % (
            ", ".join(uid_list)
        )

        return self.get_collection_uid(collection_name)

    @validate_arguments
    def create_field(self, collection_id: str, field_spec: FieldSpec):
        """Create a field in a collection. If you're trying to create an inferred field (e.g. text extraction) then use
        :py:func:\`create_inferred_field\` which wraps this function and constructs an inferred field spec for you.

        :param collection_id: The collection in which to create the field.
        :type collection_id: str
        :param field_spec: The field's definition.
        :type field_spec: :py:class:\`FieldSpec\`

        :return: None
        """

        resp = self.session.post(
            urljoin(self.api_url, "schema/ecs/file_collections::%s/fields" % (collection_id)),
            json=dict(field_spec),
        )

        if not resp.ok:
            raise APIError(resp)

    @validate_arguments
    def create_fields(self, collection_id: str, field_specs: List[FieldSpec]):
        """Create multiple fields in a collection. If you need to create multiple fields, this function is significantly
        more performant than calling :py:func:\`create_field\` in a loop.

        :param collection_id: The collection in which to create the field.
        :type collection_id: str
        :param field_specs: A list of fields to create.
        :type field_spec: List[:py:class:\`FieldSpec\`]

        :return: None
        """

        resp = self.session.post(
            urljoin(self.api_url, "schema/ecs/file_collections::%s/fields" % (collection_id)),
            json=[dict(field_spec) for field_spec in field_specs],
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
        """Create an inferred field. This is just a wrapper around :py:func:\`create_field\`.

        :param collection_id: The collection in which to create the field.
        :type collection_id: str
        :param field_name: The name of the field to create.
        :type field_name: str
        :param inferred_field_type: The inferred field type.
        :type field_name: :py:class:\`InferredFieldType\`
        :param path: Specify a path if this is a sub-field of a table. For example, if this field should be created inside of a table named `T`, path should be `["T"]`.
        :type path: List[str]

        :return: None
        """

        field_spec = inferred_field_type.build_field_spec(field_name=field_name, path=path)
        return self.create_field(collection_id, field_spec)

    @validate_arguments
    def delete_field(self, collection_id: str, field_name: str):
        """Delete a field in a collection

        :param collection_id: The collection in which to delete the field.
        :type collection_id: str
        :param field_name: The name of the field to delete.
        :type field_name: str

        :return: None
        """

        resp = self.session.delete(
            urljoin(self.api_url, "schema/ecs/file_collections::%s/fields/%s" % (collection_id, field_name)),
        )

        if not resp.ok:
            raise APIError(resp)

    @validate_arguments
    def import_fields(
        self,
        collection_id: str,
        from_collection_id: str,
    ):
        """Import field definitions from another collection.

        :param collection_id: The (destination) collection in which to add the fields.
        :type collection_id: str
        :param from_collection_id: The (source) collection from which to add the fields.
        :type from_collection_id: str

        :return: None
        """

        resp = self.session.post(
            urljoin(
                self.api_url,
                "schema",
                "ecs",
                "file_collections::" + collection_id,
                "importfields",
                "file_collections::" + from_collection_id,
            ),
        )

        if not resp.ok:
            raise APIError(resp)

    @validate_arguments
    def rename_file(self, uid: str, name: str):
        """Rename a file.

        :param uid: The uid of the file to rename
        :type uid: str
        :param name: The name to rename the file to.
        :type name: str

        :return: The uid of the updated file (it should match the uid parameter you pass in)
        """

        return self._set_field_path("files", uid, ["File", "name"], name)

    def poll_for_results(self, collection_id: str, uids: Generator[str, None, None] = []):
        """Poll a collection for new results for a set of uids. This method will block until each of the
        specified files has fully processed, so it's most often used after uploading files to a collection
        as a way to block on them processing.

        :param collection_id: The collection id to poll for results.
        :type collection_id: str
        :param uids: A list or generator of file uids to block on. You can pass in the output of :py:func:\`upload_files\` to this function directly.
        :type uids: List[str] or Generator[str, None, None]

        :return: A generator which yields results for each uid as it's available.
        """

        uid_filter = "and in(uid, %s)" % (", ".join(['"%s"' % u for u in uids])) if uids else ""
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
        while must_see:
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

    @validate_arguments
    def fetch_split_results(self, upload_uids: List[str], timeout=60):
        upload_uid_list = ", ".join(['"%s"' % u for u in upload_uids])
        query = (
            "@`files`[uid, upload_uid: File.upload_uid, child: -eq(uid, File.upload_uid)] in(File.upload_uid, %s) and child=true"
            % (upload_uid_list)
        )

        cursor = None

        must_see = set(upload_uids)
        while must_see:
            resp = self.query(query, mode="poll", cursor=cursor, timeout=timeout)
            for d in resp["data"] or []:
                if d["action"] != "insert":
                    continue

                uid = d["data"]["uid"]
                upload_uid = d["data"]["upload_uid"]
                child = d["data"]["child"]

                # This is an upload uid that has been processed, so we can remove it from the must_see list
                if upload_uid in must_see:
                    must_see.remove(upload_uid)
                yield uid

            cursor = resp["cursor"]

    @validate_arguments
    def query(self, query: str, mode: str = "iql", cursor: str = None, timeout: int = None):
        """Execute an [Impira Query Language (IQL) query](https://docs.impira.com/ref#using-impira-query-language-iql-for-advanced-queries).
        You can either run the query ad-hoc (the default) or specify "poll" for the `mode` argument to poll for changes. In
        poll-mode, you can also specify a cursor to retrieve results since a particular point-in-time. See the
        [poll docs](https://docs.impira.com/ref#polling-for-changes) for more information on polling.

        :param query: The IQL query to execute
        :type query: str
        :param mode: Either `iql` (the default) or `poll`. `iql` will run an ad-hoc query (against the current state) and `poll` will block until there are changes to the query results.
        :type mode: str
        :param cursor: If specified in `poll` mode, the command will return changes to the query results since this cursor.
        :type cursor: str, optional
        :param timeout: A timeout in seconds to run the query or poll for new results.
        :type timeout: int, optional

        :return: A generator which yields results for each uid as it's available.
        """

        args = {"query": query}
        if cursor is not None:
            args["cursor"] = cursor

        if timeout is not None:
            args["timeout"] = timeout

        start = time.time()

        while True:
            resp = self.session.post(
                urljoin(self.api_url, mode),
                json=args,
            )
            time_since = time.time() - start
            if resp.ok:
                break
            elif resp.status_code == HTTPStatus.REQUEST_TIMEOUT and time is not None and time_since < timeout:
                args["timeout"] = math.ceil(timeout - time_since)
                logging.warning("Request timed out, but still have %gs left. Will try again..." % (args["timeout"]))
                continue
            elif resp.status_code == HTTPStatus.TOO_MANY_REQUESTS and time_since < timeout - 1:
                args["timeout"] = math.ceil(timeout - 1 - time_since)
                logging.warning(
                    "Hit a rate limit, but still have %gs left. Will sleep for 1s and try again..." % (args["timeout"])
                )
                time.sleep(1)
            else:
                raise APIError(resp)

        d = resp.json()
        if "error" in d and d["error"] is not None:
            raise IQLError(d["error"])

        return resp.json()

    @validate_arguments
    def get_app_url(self, resource_type: ResourceType, resource_id: str) -> str:
        """A helper function to generate a resource URL.

        :param resource_type: The type of the resource.
        :type resource_type: :py:class:\`ResourceType\`
        :param resource_id: The value (of the resource type) to point to.
        :type resource_id: str

        :return: The https URL of the resource.
        """

        return self._build_resource_url(resource_type, resource_id, api=False, use_async=False)

    def get_collection_uid(self, collection_name: str):
        """Deprecated: Use :py:func:\`get_collection_id\` instead."""

        return self.get_collection_id(collection_name)

    def _ping(self):
        self.query("@files limit:0")

    @validate_arguments
    def _upload_multipart(self, collection_id: Optional[str], files: List[FilePath]):
        for i in range(60):
            for f in files:
                if f.uid is not None:
                    raise InvalidRequest("Unsupported: specifying a UID in a multi-part file upload (%s)" % (f.uid))

            files_body = [("file", open(f.path, "rb")) for f in files]
            data_body = [("data", json.dumps(_build_file_object(f.name, None, f.uid, f.mutate))) for f in files]

            resp = self.session.post(
                self._build_collection_url(collection_id, use_async=True),
                files=tuple(files_body),
                data=tuple(data_body),
            )
            if resp.status_code == 429:
                logging.warning("Sleeping for 2 seconds and then retrying multi-part upload...")
                time.sleep(2)
            elif not resp.ok:
                raise APIError(resp)
            else:
                return self._handle_upload_response(resp)

    @validate_arguments
    def _upload_url(self, collection_id: Optional[str], files: List[FilePath]):
        resp = self.session.post(
            self._build_collection_url(collection_id, use_async=True),
            json={"data": [_build_file_object(f.name, f.path, f.uid, f.mutate) for f in files]},
        )
        if not resp.ok:
            raise APIError(resp)

        return self._handle_upload_response(resp)

    def _handle_upload_response(self, upload_response: requests.Response):
        resp_json = upload_response.json()

        if resp_json.get("uids", None) is not None:
            return resp_json.get("uids")
        elif resp_json.get("upload_uids", None) is not None:
            return self.fetch_split_results(resp_json.get("upload_uids"))

        return []

    @validate_arguments
    def _set_field_path(self, entity_class: str, uid: str, path: List[str], data):
        resp = self.session.post(
            urljoin(self.api_url, "data", entity_class, uid, *[quote_plus(segment) for segment in path]),
            json={"data": data},
        )

        if not resp.ok:
            raise APIError(resp)

        return resp.json()["uids"]

    @validate_arguments
    def _build_collection_url(self, collection_id: Optional[str], use_async=False):
        if collection_id is not None:
            return self._build_resource_url("fc", collection_id, use_async=use_async)
        else:
            return self._build_resource_url("files", None, use_async=use_async)

    @validate_arguments
    def _build_resource_url(
        self,
        resource_type: ResourceType,
        resource_id: Optional[str],
        api=True,
        use_async=False,
    ):
        parts = [
            self.api_url if api else self.org_url,
            resource_type,
        ]

        if resource_id is not None:
            parts.append(resource_id)

        base_url = urljoin(*parts)
        if use_async:
            base_url = base_url + "?async=1"
        return base_url


def _build_file_object(name, path=None, uid=None, mutate=None):
    ret = {"File": {"name": name}}
    if path is not None:
        ret["File"]["path"] = path
    if uid is not None:
        ret["uid"] = uid
    if mutate is not None:
        ret["File"]["mutate"] = mutate.to_json()
    return ret


def urljoin(*parts):
    return "/".join([x.lstrip("/") for x in parts])
