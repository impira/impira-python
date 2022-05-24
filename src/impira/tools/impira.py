from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
import json
import os
from pydantic import BaseModel, validate_arguments
from typing import Any, Dict, List, Optional, Tuple, Type, Union
import time
from uuid import uuid4

from impira import Impira as ImpiraAPI, FieldType, InferredFieldType, parse_date

from ..cmd.utils import environ_or_required
from ..schema import schema_to_model
from ..types import (
    BBox,
    combine_bboxes,
    CheckboxLabel,
    SignatureLabel,
    DocData,
    DocSchema,
    NumberLabel,
    TextLabel,
    TimestampLabel,
    DocumentTagLabel,
)
from ..utils import batch
from .tool import Tool


class SchemaField(BaseModel):
    name: str
    path: List[str] = []
    field_type: InferredFieldType


@validate_arguments
def label_name_to_inferred_field_type(label_name: str) -> InferredFieldType:
    if label_name == "NumberLabel":
        return InferredFieldType.number
    elif label_name == "TextLabel":
        return InferredFieldType.text
    elif label_name == "TimestampLabel":
        return InferredFieldType.timestamp
    elif label_name == "CheckboxLabel":
        return InferredFieldType.checkbox
    elif label_name == "SignatureLabel":
        return InferredFieldType.signature
    elif label_name == "DocumentTagLabel":
        return InferredFieldType.document_tag
    else:
        assert False, "Unknown field label name: %s" % (label_name)


def generate_schema(doc_schema: DocSchema) -> List[SchemaField]:
    fields = []
    for field_name, value in doc_schema.fields.items():
        if isinstance(value, DocSchema):
            fields.append(SchemaField(name=field_name, field_type=InferredFieldType.table))
            for sub_field in generate_schema(value):
                sub_field.path.insert(0, field_name)
                fields.append(sub_field)
        else:
            fields.append(SchemaField(name=field_name, field_type=label_name_to_inferred_field_type(value)))

    return fields


def is_overlapping(left1, width1, left2, width2):
    return left1 + width1 >= left2 and left2 + width2 >= left1


@validate_arguments
def is_bbox_overlapping(bbox1: BBox, bbox2: BBox):
    return (
        bbox1.page == bbox2.page
        and is_overlapping(
            bbox1.left,
            bbox1.width,
            bbox2.left,
            bbox2.width,
        )
        and is_overlapping(bbox1.top, bbox1.height, bbox2.top, bbox2.height)
    )


class ImpiraWord(BaseModel):
    confidence: float
    location: BBox
    processed_word: str
    rotated: bool
    source: str
    uid: str
    word: str


class ImpiraEntity(BaseModel):
    label: str
    location: BBox
    processed: Any
    source_rivlets: List[str]
    uid: str
    word: str


FirstClassEntityLabelToFieldType = {
    "NUMBER": InferredFieldType.number,
    "MONEY": InferredFieldType.number,
    "DATE": InferredFieldType.timestamp,
    "TIME": InferredFieldType.text,
}


class CheckboxSource(BaseModel):
    BBoxes: List[BBox]


class ScalarLabel(BaseModel):
    class L(BaseModel):
        Source: Optional[Union[CheckboxSource, List[ImpiraWord]]]
        IsPrediction: bool = False
        Value: Optional[Any]

    class C(BaseModel):
        Entities: Optional[List[ImpiraEntity]]

    Label: L
    ModelVersion: Optional[int]
    Context: Optional[C]

    def is_prediction(self):
        return self.Label.IsPrediction


class RowLabel(BaseModel):
    class L(BaseModel):
        IsPrediction: bool
        Value: Dict[str, ScalarLabel]

    Label: L

    def is_prediction(self):
        return self.Label.IsPrediction


class TableLabel(BaseModel):
    class L(BaseModel):
        IsPrediction: bool
        Value: List[RowLabel]

    Label: L
    ModelVersion: int

    def is_prediction(self):
        return self.Label.IsPrediction


class EntityMap(BaseModel):
    entities: List[ImpiraEntity]
    entityIndicesByRivletUid: Optional[Dict[str, List[int]]] = None

    # This is mirrored from Impira client code. We may want to move it into the Impira SDK.
    def find_entities(self, rivlets, match_subsets=False, match_supersets=False):
        m = self.ensureEntityIndicesByRivletUid()

        entity_index_counts = defaultdict(lambda: 0)
        for r in rivlets:
            x = m.get(r.uid, [])
            for i in x:
                entity_index_counts[i] += 1

        unique_entity_indices = set()
        for i, c in entity_index_counts.items():
            if (not match_subsets and c < len(rivlets)) or (
                not match_supersets and len(self.entities[i].source_rivlets) > len(rivlets)
            ):
                continue

            unique_entity_indices.add(i)

        return [self.entities[i] for i in sorted(unique_entity_indices)]

    def ensureEntityIndicesByRivletUid(self):
        if self.entityIndicesByRivletUid is None:
            m = defaultdict(lambda: [])
            for i, e in enumerate(self.entities):
                for r in e.source_rivlets:
                    m[r].append(i)
            self.entityIndicesByRivletUid = m

        return self.entityIndicesByRivletUid


@validate_arguments
def find_overlapping_words(value, words: List[ImpiraWord]):
    if value.location is None:
        return []

    return [
        word
        for word in words
        if is_bbox_overlapping(word.location, value.location)
        # and all([w in str(value.fmt()) for w in word.word.split()])
    ]


@validate_arguments
def generate_labels(
    log,
    file_name: str,
    record,
    words: List[ImpiraWord],
    entity_map: EntityMap,
    model_versions: Dict[str, int],
) -> Dict[str, Any]:
    labels = {}
    for field_name, value in dict(record).items():
        if isinstance(value, List):
            rows = [generate_labels(log, file_name, v, words, entity_map, model_versions) for v in value]
            row_labels = [
                RowLabel(
                    Label=RowLabel.L(
                        Value=row,
                        IsPrediction=any([v.is_prediction() for v in row.values()]) or len(row.values()) == 0,
                    )
                )
                # TODO: For benchmarking, we'll want to reduce the number of labels we provide in a table
                for row in rows
            ]
            labels[field_name] = TableLabel(
                Label=TableLabel.L(
                    Value=row_labels,
                    IsPrediction=any([r.is_prediction() for r in row_labels]) or len(row_labels) == 0,
                ),
                ModelVersion=model_versions.get(field_name, 0),
            )
        elif isinstance(value, CheckboxLabel) or isinstance(value, SignatureLabel):
            scalar_label = ScalarLabel(
                Label=ScalarLabel.L(
                    Source=CheckboxSource(BBoxes=[value.location]),
                    Value={
                        "Value": value.value,
                        "State": 1 if value.value else 0,
                    },
                ),
                Context=ScalarLabel.C(Entities=[]),
                ModelVersion=model_versions.get(field_name, 0),
            )

            labels[field_name] = scalar_label
        elif value is not None:
            w = find_overlapping_words(value, words)
            candidate_entities = [e for x in w for e in entity_map.find_entities([x], match_supersets=True)]
            target_type = label_name_to_inferred_field_type(type(value).__name__)

            for e in candidate_entities:
                if FirstClassEntityLabelToFieldType.get(e.label) == target_type:
                    rivlet_set = set(e.source_rivlets)
                    w = [x for x in words if x.uid in rivlet_set]
                    break

            entities = entity_map.find_entities(w)
            if target_type != InferredFieldType.text and len(entities) == 0:
                log.warning(
                    "Field %s in %s with value %s did not match any entities, so will label it by value",
                    field_name,
                    file_name,
                    value.value,
                )
                scalar_label = ScalarLabel(
                    Label=ScalarLabel.L(Value=value.u_fmt(), Source=[]),
                    Context=ScalarLabel.C(Entities=entities),
                    IsPrediction=False,
                    ModelVersion=model_versions.get(field_name, 0),
                )
            else:
                scalar_label = ScalarLabel(
                    Label=ScalarLabel.L(Source=w),
                    Context=ScalarLabel.C(Entities=entities),
                    IsPrediction=False,
                    ModelVersion=model_versions.get(field_name, 0),
                )

            labels[field_name] = scalar_label
    return labels


def data_projection(skip_downloading_text):
    if skip_downloading_text:
        return "[uid, name: File.name, text: {words: build_array()}, entities: build_array()]"
    else:
        return "[uid, name: File.name, text: File.text, entities: File.ner.entities]"


def upload_and_retrieve_text(conn, collection_uid, f, skip_downloading_text):
    existing = conn.query(
        "@`file_collections::%s`%s name='%s' File.IsPreprocessed=true"
        % (collection_uid, data_projection(skip_downloading_text), f["name"]),
    )["data"]
    if len(existing) > 0:
        return existing[0]

    uids = conn.upload_files(collection_uid, [f])
    assert len(uids) == 1
    for i in range(10):
        while True:
            resp = conn.query(
                "@`file_collections::%s`%s uid='%s' File.IsPreprocessed=true"
                % (collection_uid, data_projection(skip_downloading_text), uids[0]),
                mode="poll",
                timeout=60,
            )

            for d in resp["data"] or []:
                if d["action"] != "insert":
                    continue

                return d["data"]


def find_path(root, *path):
    curr = root
    for p in path:
        curr = [x for x in curr["children"] if x["name"] == p][0]
    return curr


@validate_arguments
def filter_inferred_fields(fields):
    return [f for f in fields if "comment" in f and json.loads(f["comment"])["field_template"] == "inferred_field_spec"]


@validate_arguments
def fields_to_doc_schema(fields) -> DocSchema:
    ret = {}
    for f in fields:
        comment = json.loads(f["comment"])
        trainer = (
            InferredFieldType.match_trainer(comment["infer_func"]["trainer_name"]) if "infer_func" in comment else None
        )
        t = None
        if trainer == InferredFieldType.table:
            sub_fields = find_path(f, "Label", "Value", "Label", "Value").get("children", [])
            t = fields_to_doc_schema(sub_fields)
        else:
            # TODO: To distinguish between more advanced types like checkboxes
            # and signatures, we should look at the trainer directly (not the
            # scalar type).
            path = ["Label", "Value"]
            if trainer in (InferredFieldType.checkbox, InferredFieldType.signature):
                path.append("Value")
                

            scalar_type = find_path(f, *path)["fieldType"]
            if trainer == InferredFieldType.document_tag:
                t = DocumentTagLabel.__name__
            elif scalar_type == FieldType.text:
                t = TextLabel.__name__
            elif scalar_type == FieldType.number:
                t = NumberLabel.__name__
            elif scalar_type == FieldType.timestamp:
                t = TimestampLabel.__name__
            elif scalar_type == FieldType.bool and trainer == InferredFieldType.checkbox:
                t = CheckboxLabel.__name__
            elif scalar_type == FieldType.bool and trainer == InferredFieldType.signature:
                t = SignatureLabel.__name__
            else:
                assert False, "Unknown scalar type: %s" % (scalar_type)
        ret[f["name"]] = t
    return DocSchema(fields=ret)


@validate_arguments
def row_to_record(row, doc_schema: DocSchema) -> Any:
    d = {}
    for field_name, field_type in doc_schema.fields.items():
        label = None
        value = row.get(field_name, None)
        if isinstance(field_type, DocSchema):
            table_rows = [row_label["Label"]["Value"] for row_label in value["Label"]["Value"]]
            label = [x for x in [row_to_record(tr, field_type) for tr in table_rows] if x is not None]
            if not label:
                continue
        elif value is not None:
            impira_label = ScalarLabel.parse_obj(value)
            if impira_label.Label.IsPrediction:
                continue
            location = (
                combine_bboxes(*[x.location for x in impira_label.Label.Source])
                if len(impira_label.Label.Source) > 0
                else None
            )
            scalar = impira_label.Label.Value
            if scalar is not None:
                if field_type == "TimestampLabel":
                    scalar = parse_date(scalar)
                elif field_type == "CheckboxLabel" or field_type == "SignatureLabel":
                    scalar = scalar["Value"]  # Checkboxes are nested inside of an extra 'Value'
            label = {"location": location, "value": scalar}
        d[field_name] = label

    if len(d) == 0:
        return None

    M = schema_to_model(doc_schema)
    return M(**d)


@validate_arguments
def row_to_fname(row, use_original_filename) -> str:
    if use_original_filename:
        return row["File"]["name"]
    else:
        fname, ext = row["File"]["name"].rsplit(".", 1)
        return fname + "-" + row["uid"] + "." + ext


class Impira(Tool):
    class Config(BaseModel):
        api_token: str
        org_name: str
        base_url: str

    @staticmethod
    def add_arguments(parser):
        parser.add_argument("--api-token", **environ_or_required("IMPIRA_API_TOKEN"))
        parser.add_argument("--org-name", **environ_or_required("IMPIRA_ORG_NAME"))
        parser.add_argument("--base-url", **environ_or_required("IMPIRA_BASE_URL", "https://app.impira.com"))

    @validate_arguments
    def __init__(self, config: Config):
        self.config = config

    def _conn(self):
        return ImpiraAPI(
            org_name=self.config.org_name,
            api_token=self.config.api_token,
            base_url=self.config.base_url,
        )

    @validate_arguments
    def run(
        self,
        doc_schema: DocSchema,
        entries: List[DocData],
        collection_prefix: str,
        parallelism: int,
        existing_collection_uid: Optional[str] = None,
        skip_type_inference=False,
        skip_upload=False,
        add_files=False,
        skip_missing_files=False,
        skip_new_fields=False,
        collection_name=None,
        max_fields=-1,
        max_files=-1,
    ):
        log = self._log()

        schema = generate_schema(doc_schema)

        skip_downloading_text = all([t == "DocumentTagLabel" for t in doc_schema.fields.values()])

        conn = self._conn()

        if existing_collection_uid is None:
            assert not (skip_upload and not add_files), "Cannot skip uploading if we're creating a new collection."

            if collection_name is None:
                collection_name = "%s-%s" % (
                    collection_prefix,
                    uuid4(),
                )

            log.info("Creating collection %s" % (collection_name))
            collection_uid = conn.create_collection(collection_name)

        else:
            collection_uid = existing_collection_uid

        log.info("You can visit the collection at: %s" % (conn.get_app_url("fc", collection_uid)))

        assert (not add_files) or skip_upload, "Cannot add existing files to the collection unless you skip upload"

        if max_files != -1:
            new_entries = [e for e in entries]
            new_entries.sort(key=lambda e: e.record is None)  # Place the rows with records up front
            entries = new_entries[:max_files]

        if not skip_upload:
            files = [{"path": e.url or str(e.fname), "name": e.fname.name} for e in entries]

            log.info("Uploading %d files", len(files))
            with ThreadPoolExecutor(max_workers=parallelism) as t:
                file_data = [
                    x
                    for x in t.map(
                        lambda f: upload_and_retrieve_text(conn, collection_uid, f, skip_downloading_text),
                        files,
                    )
                ]
        else:
            while True:
                uids = {
                    r["name"]: r
                    for r in conn.query(
                        "@`file_collections::%s`%s" % (collection_uid, data_projection(skip_downloading_text))
                    )["data"]
                }

                if add_files:
                    missing_files = [e.fname.name for e in entries if e.fname.name not in uids]
                    if len(missing_files) == 0:
                        break
                    file_filter = "in(File.name, %s)" % (
                        ",".join(['"%s"' % n.replace('"', '\\"') for n in missing_files])
                    )
                    missing_file_uids = {
                        r["name"]: r["uid"]
                        for r in conn.query("@files[name: File.name, uid] %s" % (file_filter))["data"]
                    }

                    assert len(missing_file_uids) == len(
                        missing_files
                    ), "Only found %d/%d files in the org. Missing: %s" % (
                        len(missing_file_uids),
                        len(missing_files),
                        [x for x in missing_files if x not in missing_file_uids],
                    )
                    log.info("Adding %d files to %s", len(missing_file_uids), collection_uid)
                    conn.add_files_to_collection(collection_uid, list(missing_file_uids.values()))

                else:
                    break

            if skip_missing_files:
                entries = [e for e in entries if e.fname.name in uids]

            file_data = [uids[e.fname.name] for e in entries]

        log.info("File uids: %s", [r["uid"] for r in file_data])

        # Now, just trim it down to the labeled entries
        labeled_entries = []
        labeled_files = []
        for i, e in enumerate(entries):
            if e.record is not None:
                labeled_entries.append(e)
                labeled_files.append(file_data[i])

        if len(labeled_entries) == 0:
            log.warning("No records have labels. Stopping now that uploads have completed.")
            return

        model_versions = {
            row["field_name"]: row["model_version"]
            for row in conn.query(
                """@__system::ecs name='file_collections::%s'
            [.: flatten(fields[field, infer_func])]
            [field_name: field.name,
                model_version: join_one(__training_membership, infer_func.model_name, model_name).model_version
            ] -model_version=null"""
                % (collection_uid)
            )["data"]
        }

        labels = []
        for e, fd in zip(labeled_entries, labeled_files):
            entity_map = EntityMap(entities=fd["entities"])
            labels.append(
                generate_labels(
                    log,
                    fd["name"],
                    e.record,
                    fd["text"]["words"],
                    entity_map,
                    model_versions,
                )
            )

        schema_resp = conn.query("@file_collections::%s limit:0" % (collection_uid))
        current_fields = fields_to_doc_schema(filter_inferred_fields(schema_resp["schema"]["children"])).fields

        new_fields = []
        field_specs = []
        field_names_to_update = set()
        for f in schema[:max_fields] if max_fields != -1 else schema:
            field_type = f.field_type
            first_labels = labels[0]
            if f.name in first_labels and isinstance(first_labels[f.name], ScalarLabel) and not skip_type_inference:
                entities = first_labels[f.name].Context.Entities
                unique_entity_types = set(
                    [field_type]
                    + [
                        FirstClassEntityLabelToFieldType[e.label]
                        for e in entities
                        if e.label in FirstClassEntityLabelToFieldType
                    ]
                )

                if InferredFieldType.timestamp in unique_entity_types:
                    field_type = InferredFieldType.timestamp
                elif InferredFieldType.number in unique_entity_types:
                    field_type = InferredFieldType.number

            if f.name in current_fields or (len(f.path) > 0 and f.path[0] in current_fields):
                existing_field = current_fields.get(f.name) or current_fields.get(f.path[0])
                if isinstance(existing_field, DocSchema):
                    field_names_to_update.add(f.name)
                    continue

                existing_label_type = label_name_to_inferred_field_type(existing_field)
                if existing_label_type != field_type:
                    log.warning(
                        "Field %s already created with type %s, but we're setting it to a value of type %s. You may want to delete it.",
                        f.name,
                        existing_label_type,
                        field_type,
                    )
                else:
                    field_names_to_update.add(f.name)
            elif not skip_new_fields:
                log.debug(
                    "Creating field %s: type=%s, path=%s",
                    f.name,
                    field_type,
                    f.path,
                )

                new_fields.append(f)
                field_specs.append(field_type.build_field_spec(f.name, f.path))
                field_names_to_update.add(f.name)

        log.info("Creating fields: %s" % (field_names_to_update))

        if len(field_specs) > 0:
            conn.create_fields(collection_uid, field_specs)

        fields_to_update = [f for f in schema if len(f.path) == 0 and f.name in field_names_to_update]

        # Snapshot the model version and its cursor for new fields. Old fields are not going to change
        # their values (unless the labels have changed, which is not something we distinguish).
        expected_increment_query = "@`file_collections::%s`[increment: %s]" % (
            collection_uid,
            " + ".join(
                ["0"]
                + [
                    "(SUM(IF(`%s`.Label.IsPrediction or `%s`=null, 1, %d-`%s`.ModelVersion)))"
                    % (
                        f.name,
                        f.name,
                        model_versions.get(f.name, 0),
                        f.name,
                    )
                    for (f) in fields_to_update
                ]
            ),
        )

        log.info("Running expected increment query %s", expected_increment_query)
        increment = conn.query(expected_increment_query)["data"][0]["increment"]

        mv_query = "@`file_collections::%s`[sum_mv: %s]" % (
            collection_uid,
            " + ".join(["0"] + ["SUM(`%s`.`ModelVersion`)" % (f.name) for f in fields_to_update]),
        )

        # Unfortunately, polling doesn't work for "min" queries, so we just run a normal query
        log.info("Running model version query %s", mv_query)
        resp = conn.query(mv_query)["data"][0]
        target = resp["sum_mv"] + increment

        log.info(
            "Current minimum model version total %s. Target is %s across %d fields",
            resp["sum_mv"],
            target,
            len(fields_to_update),
        )

        log.info("Running update on %d files" % len(labeled_files))
        conn.update(
            collection_uid,
            [
                {
                    **{"uid": fd["uid"]},
                    **{
                        field_path: label.dict(exclude_none=True)
                        for field_path, label in ld.items()
                        if field_path in field_names_to_update
                    },
                }
                for (fd, ld) in zip(labeled_files, labels)
            ],
        )
        log.info("Done running update on %d files. Models will now update!" % len(labeled_files))

        # This code is currently too brittle to be useful, since model versions aren't a reliable way of knowing
        # when evaluation has finished. We can reintroduce it once we have a better strategy in place.
        #
        #        while True:
        #            resp = conn.query(
        #                mv_query + " [sum_mv] sum_mv >= %d" % (target),
        #            )
        #            try:
        #                resp = resp["data"][0]
        #                break
        #            except Exception as e:
        #                time.sleep(1)
        #                continue
        #
        #        # TODO: We know this undercounts a bit, so we should probably check the "spinner" query too
        #
        #        log.debug(
        #            "Current minimum model version %s",
        #            resp["sum_mv"],
        #        )
        log.info("Done!")

    @validate_arguments
    def snapshot(self, collection_uid: str, use_original_filenames=False, labeled_files_only=False):
        log = self._log()

        conn = self._conn()
        resp = conn.query("@file_collections::%s" % (collection_uid))

        doc_schema = fields_to_doc_schema(filter_inferred_fields(resp["schema"]["children"]))
        records = [
            {
                "url": row["File"]["download_url"],
                "name": row_to_fname(row, use_original_filenames),
                "record": row_to_record(row, doc_schema),
            }
            for row in resp["data"]
        ]

        if labeled_files_only:
            records = [r for r in records if r["record"] is not None]

        assert len(records) == len(set([r["name"] for r in records])), "Expected each filename to be unique"

        return doc_schema, records

    @validate_arguments
    def snapshot_collections(self, use_original_filenames=False):
        log = self._log()

        conn = self._conn()
        files = conn.query("@files[uid, File: File[download_url, name]] -`File type`=Data")["data"]
        collections = conn.query(
            "@file_collection_contents[collection_uid, files: array_agg(file_uid)] -collection=null"
        )["data"]

        collection_names = {}
        for row in conn.query("@file_collection_contents[collection_uid, name: collection.name] -collection=null")[
            "data"
        ]:
            collection_names[row["collection_uid"]] = row["name"]

        file_membership = {}
        for c in collections:
            for f in c["files"]:
                if f not in file_membership:
                    file_membership[f] = []
                file_membership[f].append(c["collection_uid"])

        sampled = {}
        # For each collection, pick up to two files that belong to that collection
        for c in collections:
            for f in [f for f in c["files"] if len(file_membership[f]) == 1][:2]:
                sampled[f] = c["collection_uid"]

        doc_schema = DocSchema(
            fields={
                "Doc tag": DocumentTagLabel.__name__,
                "Sampled tag": DocumentTagLabel.__name__,
            }
        )

        records = [
            {
                "url": row["File"]["download_url"],
                "name": row_to_fname(row, use_original_filenames),
                "record": {
                    "Doc tag": DocumentTagLabel(value=collection_names[file_membership[row["uid"]][0]])
                    if row["uid"] in file_membership and len(file_membership[row["uid"]]) == 1
                    else DocumentTagLabel(value=None) if row["uid"] not in file_membership 
                    else None,
                    "Sampled tag": DocumentTagLabel(value=collection_names[sampled[row["uid"]]])
                    if row["uid"] in sampled
                    else None,
                },
            }
            for row in files
        ]

        assert len(records) == len(set([r["name"] for r in records])), "Expected each filename to be unique"

        return doc_schema, records
