from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
import json
import os
import pathlib
from pydantic import BaseModel, validate_arguments, ValidationError
from typing import Any, Dict, List, Optional, Tuple, Type, Union
import time
from uuid import uuid4

from .. import Impira as ImpiraAPI, FieldType, InferredFieldType, parse_date, APIError

from ..cmd.utils import environ_or_required
from ..schema import schema_to_model
from ..types import (
    Location,
    combine_locations,
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
def is_bbox_overlapping(bbox1: Location, bbox2: Location):
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
    location: Location
    processed_word: str
    rotated: Optional[bool]
    source: Optional[str]
    uid: Optional[str]
    word: str


class ImpiraEntity(BaseModel):
    label: str
    location: Location
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
    BBoxes: List[Location]


class DocumentTagValue(BaseModel):
    class L(BaseModel):
        Value: Optional[str]
        IsPrediction: bool = False

    Label: L


class ScalarLabel(BaseModel):
    class L(BaseModel):
        Source: Optional[Union[CheckboxSource, List[ImpiraWord]]]
        IsPrediction: bool = False
        IsConfident: Optional[bool] = None
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
        IsConfident: Optional[bool] = None
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
def find_overlapping_words(value, word_map: Dict[str, ImpiraWord], words: List[ImpiraWord]):
    if value.location is None:
        return []

    if len(value.location.uids) > 0:
        uid_words = [word_map[uid] for uid in value.location.uids if uid in word_map]
        if len(uid_words) == len(value.location.uids):
            return uid_words

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
    word_map: Dict[str, ImpiraWord],
    entity_map: EntityMap,
    model_versions: Dict[str, int],
    empty_labels: bool = False,
) -> Dict[str, Any]:
    labels = {}
    for field_name, value in dict(record).items():
        if isinstance(value, List):
            rows = [
                generate_labels(log, file_name, v, words, word_map, entity_map, model_versions, empty_labels)
                for v in value
            ]
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
        elif isinstance(value, (CheckboxLabel, SignatureLabel)):
            scalar_label = ScalarLabel(
                Label=ScalarLabel.L(
                    Source=CheckboxSource(BBoxes=[value.location] if value.location else []),
                    Value={
                        "Value": value.value == 1 if value.value is not None else None,
                        "State": value.value,
                    },
                ),
                Context=ScalarLabel.C(Entities=[]),
                IsPrediction=False,
                ModelVersion=model_versions.get(field_name, 0),
            )

            labels[field_name] = scalar_label
        elif isinstance(value, DocumentTagLabel):
            scalar_label = ScalarLabel(
                Label=ScalarLabel.L(
                    Source=[],
                    Value=[DocumentTagValue(Label={"Value": x, "IsPrediction": False}) for x in value.fmt()],
                ),
                Context=ScalarLabel.C(Entities=[]),
                IsPrediction=False,
                ModelVersion=model_versions.get(field_name, 0),
            )
            labels[field_name] = scalar_label
        elif value is not None and value.value is not None:
            w = find_overlapping_words(value, word_map, words)
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
        elif (value is None and empty_labels) or (value is not None and value.location is None):
            labels[field_name] = ScalarLabel(
                Label=ScalarLabel.L(Source=[]),
                Context=ScalarLabel.C(Entities=[]),
                IsPrediction=False,
                ModelVersion=model_versions.get(field_name, 0),
            )
    return labels


def data_projection(skip_downloading_text):
    if skip_downloading_text:
        return "[uid, name: File.name, text: {words: build_array()}, entities: build_array()]"
    else:
        return "[uid, name: File.name, text: File.text, entities: File.ner.entities]"


def escape_name(name):
    return name.replace("'", "\\'")


def upload_files(conn, collection_uid, b):
    names = ", ".join([f"'{escape_name(f['name'])}'" for f in b])
    files_by_name = {}
    for f in b:
        files_by_name[f["name"]] = f

    all_uids = {}
    existing = conn.query(f"@`file_collections::{collection_uid}`[uid, name: File.name] in(name, {names})")["data"]
    for e in existing:
        all_uids[e["name"]] = e["uid"]
        del files_by_name[e["name"]]

    fb_v = [x for x in files_by_name.items()]
    upload_uids = conn.upload_files(collection_uid, [f for (_, f) in fb_v])
    for (uid, (fname, _)) in zip(upload_uids, fb_v):
        all_uids[fname] = uid

    uids = set(upload_uids)
    cursor = None
    for i in range(10):
        if len(uids) == 0:
            return [all_uids[f["name"]] for f in b]

        uid_filter = "in(uid, %s)" % (", ".join(['"%s"' % u for u in uids]))
        resp = conn.query(
            f"@`file_collections::{collection_uid}`[uid] {uid_filter} File.IsPreprocessed=true",
            mode="poll",
            timeout=360,
            cursor=cursor,
        )
        cursor = resp["cursor"]

        for d in resp["data"] or []:
            if d["action"] != "insert":
                continue

            uid = d["data"]["uid"]
            uids.remove(uid)
    assert False, "Poll timed out after an hour"


def retrieve_text(conn, collection_uid, uid_batch, skip_downloading_text):
    results = {}

    uids = set(uid_batch)
    cursor = None
    for i in range(10):
        if len(uids) == 0:
            return [results[uid] for uid in uid_batch]

        uid_filter = "in(uid, %s)" % (", ".join(['"%s"' % u for u in uids]))
        resp = conn.query(
            f"@`file_collections::{collection_uid}`{data_projection(skip_downloading_text)} {uid_filter} File.IsPreprocessed=true",
            mode="poll",
            timeout=360,
            cursor=cursor,
        )
        cursor = resp["cursor"]

        for d in resp["data"] or []:
            if d["action"] != "insert":
                continue

            uid = d["data"]["uid"]
            results[uid] = d["data"]
            uids.remove(uid)

    assert False, "Poll timed out after an hour"


# NOTE: Deprecated
def upload_and_retrieve_text(conn, collection_uid, f, skip_downloading_text):
    existing = conn.query(
        "@`file_collections::%s`%s name='%s' File.IsPreprocessed=true"
        % (collection_uid, data_projection(skip_downloading_text), f["name"].replace("'", "\\'")),
        timeout=360,
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
                timeout=360,
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
        try:
            trainer = (
                InferredFieldType.match_trainer(comment["infer_func"]["trainer_name"])
                if "infer_func" in comment
                else None
            )
        except AssertionError:
            # If this is an unknown trainer, e.g. table v1, just skip the field
            continue

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
def maybe_get_location(label: ScalarLabel, field_type: str) -> Optional[Location]:
    if label.Label.Source is None:
        return None

    uids = []

    if field_type in ("CheckboxLabel", "SignatureLabel"):
        locations = label.Label.Source.BBoxes
    elif field_type in ("TextLabel", "NumberLabel", "TimestampLabel"):
        locations = [l.location for l in label.Label.Source]
        uids = [l.uid for l in label.Label.Source]
    elif field_type == "DocumentTagLabel":
        locations = None
    else:
        raise ValueError(f"Unable to retrieve location from label of type {field_type} ({label = })")

    if locations:
        loc = combine_locations(locations)
        loc.uids = uids
        return loc
    return None


@validate_arguments
def maybe_get_value(label: ScalarLabel, field_type: str) -> Optional[Any]:
    value = label.Label.Value
    if field_type in ("CheckboxLabel", "SignatureLabel"):
        value = value["State"]
    elif field_type == "TimestampLabel" and value is not None:
        value = parse_date(value)
    elif field_type == "DocumentTagLabel":
        value = [e["Label"]["Value"] for e in value if e["Label"] and e["Label"]["Value"] is not None]

    return value


@validate_arguments
def row_to_record(
    log,
    row,
    doc_schema: DocSchema,
    allow_predictions: bool,
    allow_low_confidence: bool,
    reverse_field_mapping: Dict[str, str],
) -> Any:
    d = {}
    for mapped_name, field_type in doc_schema.fields.items():
        field_name = reverse_field_mapping.get(mapped_name, mapped_name)

        label = None
        field = row.get(field_name)
        if isinstance(field_type, DocSchema):
            label = None
            if field is not None and field.get("Label") and field.get("Label").get("Value"):
                table_rows = [
                    row_label["Label"]["Value"]
                    for row_label in field["Label"]["Value"]
                    if row_label and row_label.get("Label")
                ]
                label = [
                    x
                    for x in [
                        row_to_record(
                            log, tr, field_type, allow_predictions, allow_low_confidence, reverse_field_mapping
                        )
                        for tr in table_rows
                    ]
                    if x is not None
                ]
            if not label:
                continue
        elif field is not None and field.get("Label") is not None:
            try:
                impira_label = ScalarLabel.parse_obj(field)
            except ValidationError as e:
                log.warning(
                    f"Record with uid={row['uid']} has an invalid label for field `{field_name}`: {e}. Skipping..."
                )
                continue

            if impira_label.Label.IsPrediction and not (
                allow_predictions and (allow_low_confidence or impira_label.Label.IsConfident)
            ):
                continue

            location = maybe_get_location(impira_label, field_type)

            try:
                value = maybe_get_value(impira_label, field_type)
            except Exception as e:
                log.warning(
                    f"Record with uid={row['uid']} has an invalid label for field `{field_name}`. Failed to parse: {e}. Skipping..."
                )
                continue

            label = {"location": location, "value": value}

        d[mapped_name] = label

    if len(d) == 0:
        return None

    M = schema_to_model(doc_schema)
    try:
        return M(**d)
    except ValidationError as e:
        log.warning(f"Record with uid={row['uid']} has an invalid label: {e}")
        return None


@validate_arguments
def row_to_fname(row, use_original_filename) -> str:
    file_name = row["File"].get("name") or ""
    uid = row["uid"]

    if use_original_filename:
        return file_name
    else:
        if "." not in file_name:
            fname = file_name + "-" + uid
        else:
            fname_, ext = file_name.rsplit(".", 1)
            fname = fname_ + "-" + uid + "." + ext
        return fname.replace("/", "_")


def fname_filter(fnames):
    return "in(File.name, %s)" % (",".join(['"%s"' % n.replace('"', '\\"') for n in fnames]))


RETRIES = 10


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
        empty_labels=False,
        collection_name=None,
        max_fields=-1,
        first_file=0,
        max_files=-1,
        batch_size=50,
        first_batch=0,
        cache_dir: pathlib.Path = None,
    ):
        log = self._log()

        if cache_dir:
            cache_dir.mkdir(parents=True, exist_ok=True)

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

        new_entries = [e for e in entries]
        new_entries.sort(key=lambda e: e.record is None)  # Place the rows with records up front
        entries = new_entries[first_file:]
        if max_files != -1:
            entries = new_entries[:max_files]

        if not skip_upload:
            files = [{"path": e.url or str(e.fname), "name": e.fname.name} for e in entries]

            log.info("Uploading %d files", len(files))
            with ThreadPoolExecutor(max_workers=parallelism) as t:
                uid_batches = [
                    x for x in t.map(lambda f: upload_files(self._conn(), collection_uid, f), batch(files, 20))
                ]

            log.info("Retrieving text for %d files", len(files))
            with ThreadPoolExecutor(max_workers=parallelism) as t:
                file_data = [
                    x
                    for results_batch in t.map(
                        lambda b: retrieve_text(self._conn(), collection_uid, b, skip_downloading_text),
                        uid_batches,
                    )
                    for x in results_batch
                ]

            if cache_dir:
                for f, record in zip(files, file_data):
                    cache_file = cache_dir / f"{f['name']}.json"
                    with open(cache_file, "w") as f:
                        json.dump(record, f)
        else:
            all_fnames = [e.fname.name for e in entries]
            cached = 0
            missing_file_uids = {}
            log.info("Retrieving text for %d files", len(all_fnames))
            while True:
                uids = {}
                for b in batch(all_fnames, 50):
                    remaining = []

                    if cache_dir:
                        for name in b:
                            cache_file = cache_dir / f"{name}.json"
                            if name not in missing_file_uids and cache_file.exists():
                                with open(cache_file, "r") as f:
                                    record = json.load(f)
                                    if record is not None:
                                        uids[name] = record
                                cached += 1
                            else:
                                remaining.append(name)

                    new_records = (
                        {
                            r["name"]: r
                            for r in conn.query(
                                "@`file_collections::%s`%s %s"
                                % (collection_uid, data_projection(skip_downloading_text), fname_filter(remaining))
                            )["data"]
                        }
                        if remaining
                        else {}
                    )

                    uids.update(new_records)
                    if cache_dir:
                        for fname in remaining:
                            cache_file = cache_dir / f"{fname}.json"
                            if fname in new_records:
                                with open(cache_file, "w") as f:
                                    json.dump(new_records[fname], f)
                            else:
                                with open(cache_file, "w") as f:
                                    json.dump(None, f)

                if add_files:
                    missing_files = [e.fname.name for e in entries if e.fname.name not in uids]
                    if len(missing_files) == 0:
                        break
                    file_filter = fname_filter(missing_files)
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

            log.info("Done retrieving text for %d files (%d cached, %d final)", len(all_fnames), cached, len(uids))
            if skip_missing_files:
                entries = [e for e in entries if e.fname.name in uids]

            file_data = [uids[e.fname.name] for e in entries]

        log.info("File uids: %s", [r["uid"] for r in file_data])

        # Now, just trim it down to the labeled entries
        labeled_entries = []
        labeled_files = []
        seen_uids = set()
        for i, e in enumerate(entries):
            if e.record is not None and file_data[i]["uid"] not in seen_uids:
                labeled_entries.append(e)
                labeled_files.append(file_data[i])
                seen_uids.add(file_data[i]["uid"])

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
        for i, (e, fd) in enumerate(zip(labeled_entries, labeled_files)):
            try:
                words = fd["text"]["words"]
                word_map = {w["uid"]: w for w in words}
                entity_map = EntityMap(entities=fd["entities"] or [])
                labels.append(
                    generate_labels(
                        log,
                        fd["name"],
                        e.record,
                        fd["text"]["words"],
                        word_map,
                        entity_map,
                        model_versions,
                        empty_labels,
                    )
                )
            except Exception as e:
                log.warning(f"Unable to process record (uid={fd['uid']}): %s", e)
                labels.append({})

        schema_resp = conn.query("@file_collections::%s limit:0" % (collection_uid))
        current_fields = fields_to_doc_schema(filter_inferred_fields(schema_resp["schema"]["children"])).fields

        new_fields = []
        field_specs = []
        field_names_to_update = set()
        for f in schema[:max_fields] if max_fields != -1 else schema:
            field_type = f.field_type

            if not skip_type_inference:
                narrow_types = set()
                for label in labels:
                    if f.name in label and isinstance(label[f.name], ScalarLabel):
                        entities = label[f.name].Context.Entities
                        unique_entity_types = set(
                            [field_type]
                            + [
                                FirstClassEntityLabelToFieldType[e.label]
                                for e in entities
                                if e.label in FirstClassEntityLabelToFieldType
                            ]
                        )

                        if InferredFieldType.timestamp in unique_entity_types:
                            narrow_type = InferredFieldType.timestamp
                        elif InferredFieldType.number in unique_entity_types:
                            narrow_type = InferredFieldType.number
                        else:
                            narrow_type = field_type

                        narrow_types.add(narrow_type)
                        if narrow_type == InferredFieldType.text:
                            break

                for weak_type in [InferredFieldType.text, InferredFieldType.number, InferredFieldType.timestamp]:
                    if weak_type in narrow_types:
                        field_type = weak_type
                        break

            if f.name in current_fields or (len(f.path) > 0 and f.path[0] in current_fields):
                existing_field = current_fields.get(f.name) or current_fields.get(f.path[0])
                if isinstance(existing_field, DocSchema):
                    field_names_to_update.add(f.name)
                    continue

                existing_label_type = label_name_to_inferred_field_type(existing_field)
                if existing_label_type != field_type and not skip_type_inference:
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
            for b in batch(field_specs, n=5):
                conn.create_fields(collection_uid, b)

        fields_to_update = [f for f in schema if len(f.path) == 0 and f.name in field_names_to_update]

        log.info("Running update on %d files" % len(labeled_files))

        # Batch the updates into chunks and retry a few times because of deadlock-issues
        batches = [b for b in batch([x for x in zip(labeled_files, labels)], n=batch_size)]
        for b_idx, b in enumerate(batches):
            if b_idx < first_batch:
                continue
            log.info("Updating batch %d/%d", b_idx, len(batches) - 1)
            processed = 0
            for i in range(RETRIES):
                if i > 0:
                    log.warning("Sleeping for 1 second...")
                    time.sleep(1)
                try:
                    mini_batches = [x for x in batch(b[processed:], n=max(1, batch_size // (i + 1)))]

                    for mb_idx, mb in enumerate(mini_batches):
                        if len(mini_batches) > 1:
                            log.info("Doing a mini-update (attempt %d): %d/%d", i, mb_idx, len(mini_batches) - 1)
                        update_record = [
                            {
                                **{"uid": fd["uid"]},
                                **{
                                    field_path: label.dict(exclude_none=True)
                                    for field_path, label in ld.items()
                                    if field_path in field_names_to_update
                                },
                            }
                            for (fd, ld) in mb
                        ]
                        conn.update(collection_uid, update_record)
                        processed += len(mb)
                    if i > 0:
                        log.info("Success!")
                    break
                except APIError as e:
                    if i < RETRIES - 1:
                        log.warning("Failed to update. Will retry up to %d more times: %s" % (10 - i - 1, e))
                    else:
                        raise

        log.info("Done running update on %d files. Models will now update!" % len(labeled_files))

    @validate_arguments
    def snapshot(
        self,
        collection_uid: str,
        use_original_filenames=False,
        labeled_files_only=False,
        filter_collection_uid=None,
        label_filter=None,
        allow_low_confidence=False,
        field_mapping: Optional[Dict[str, str]] = {},
    ):
        log = self._log()

        conn = self._conn()

        collection_filter = (
            f"-join_one(`file_collections::{filter_collection_uid}`, uid, uid)=null" if filter_collection_uid else ""
        )

        allow_predictions = f"{label_filter}" if label_filter else "false"

        resp = conn.query(
            "@`file_collections::%s`[.*, __allow_predictions: %s] %s"
            % (collection_uid, allow_predictions, collection_filter)
        )

        doc_schema = fields_to_doc_schema(filter_inferred_fields(resp["schema"]["children"]))

        if field_mapping is not None:
            doc_schema.fields = {field_mapping.get(n): t for (n, t) in doc_schema.fields.items() if n in field_mapping}
        else:
            field_mapping = {}
        reverse_field_mapping = {v: k for (k, v) in field_mapping.items()}
        records = [
            {
                "url": row["File"]["download_url"],
                "name": row_to_fname(row, use_original_filenames),
                "record": row_to_record(
                    log, row, doc_schema, bool(row["__allow_predictions"]), allow_low_confidence, reverse_field_mapping
                ),
            }
            for row in resp["data"]
        ]

        if labeled_files_only:
            records = [r for r in records if r["record"] is not None]

        assert len(records) == len(set([r["name"] for r in records])), "Expected each filename to be unique"

        return doc_schema, records

    @validate_arguments
    def snapshot_collections(
        self, use_original_filenames=False, max_files_per_collection=-1, num_samples=2, collection_filter=None
    ):
        log = self._log()

        full_collection_filter = "-collection=null"
        if collection_filter:
            full_collection_filter += " in(collection_uid, %s)" % (
                ", ".join(["'%s'" % (uid) for uid in collection_filter])
            )

        conn = self._conn()
        files = conn.query("@files[uid, File: File[download_url, name]] -File.download_url=null -`File type`=Data")[
            "data"
        ]
        collections = conn.query(
            f"@file_collection_contents[collection_uid, files: array_agg(file_uid)] {full_collection_filter}"
        )["data"]

        collection_names = {}
        for row in conn.query(
            f"@file_collection_contents[collection_uid, name: collection.name] {full_collection_filter}"
        )["data"]:
            collection_names[row["collection_uid"]] = row["name"]

        file_membership = {}
        for c in collections:
            collection = c["files"] if max_files_per_collection < 0 else c["files"][:max_files_per_collection]
            for f in collection:
                if f not in file_membership:
                    file_membership[f] = []
                file_membership[f].append(c["collection_uid"])

        sampled = {}
        # For each collection, pick up to two files that belong to that collection
        for c in collections:
            valid_files = [f for f in c["files"] if f in file_membership and len(file_membership[f]) == 1]
            if num_samples > len(valid_files):
                log.warning(
                    f"The collection {c['collection_uid']} has fewer files than the requested number of samples "
                    f"({len(valid_files)} < {num_samples}), so sampling all files in the collection. It's "
                    "recommended that you either lower the number of samples or add more files to the collection."
                )

            num_samples_ = min(len(valid_files), num_samples)
            for f in valid_files[:num_samples_]:
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
                    "Doc tag": DocumentTagLabel(value=[collection_names[x] for x in file_membership[row["uid"]]])
                    if row["uid"] in file_membership and len(file_membership[row["uid"]]) == 1
                    else DocumentTagLabel(value=[])
                    if row["uid"] not in file_membership
                    else None,
                    "Sampled tag": DocumentTagLabel(value=[collection_names[sampled[row["uid"]]]])
                    if row["uid"] in sampled
                    else None,
                },
            }
            for row in files
            if row["uid"] in file_membership
        ]

        assert len(records) == len(set([r["name"] for r in records])), "Expected each filename to be unique"

        return doc_schema, records
