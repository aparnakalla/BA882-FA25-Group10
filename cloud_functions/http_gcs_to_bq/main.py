import json
import os
from typing import Dict, Any, List
from google.cloud import storage, bigquery

# ---------------- CONFIGURATION ----------------
PROJECT_ID = os.environ.get("PROJECT_ID") or "christina-ba882-fall25"
BQ_DATASET = os.environ.get("BQ_DATASET") or "data_from_gcs_to_bq"
BUCKET_NAME = os.environ.get("BUCKET_NAME") or "ba882-team10-bucket"

storage_client = storage.Client(project=PROJECT_ID)
bq_client = bigquery.Client(project=PROJECT_ID)


# ---------------- HELPERS ----------------
def _flatten_mbta_json(obj: Dict[str, Any], route_hint: str | None) -> List[Dict[str, Any]]:
    data = obj.get("data", [])
    out: List[Dict[str, Any]] = []

    for item in data:
        rec = {"id": item.get("id"), "type": item.get("type")}
        for k, v in (item.get("attributes", {}) or {}).items():
            rec[k] = v
        for rel_name, rel_body in (item.get("relationships", {}) or {}).items():
            rid = None
            if "data" in rel_body:
                rdata = rel_body["data"]
                if isinstance(rdata, dict):
                    rid = rdata.get("id")
                elif isinstance(rdata, list) and rdata:
                    rid = ",".join([str(x.get("id")) for x in rdata if isinstance(x, dict)])
            rec[f"{rel_name}_id"] = rid
        if route_hint:
            rec["route_hint"] = route_hint
        out.append(rec)
    return out


def _infer_table_from_path(gcs_name: str) -> tuple[str, str | None]:
    parts = gcs_name.split("/")
    if len(parts) < 2:
        raise ValueError(f"Unexpected object path: {gcs_name}")
    table = parts[1]
    route_hint = None
    for p in parts:
        if p.startswith("route="):
            route_hint = p.split("=", 1)[1]
            break
    return table, route_hint


def _insert_rows(table_id: str, rows: List[Dict[str, Any]]):
    if not rows:
        return 0
    errors = bq_client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors[:3]}")
    return len(rows)


def _process_single_file(bucket_name: str, name: str):
    """Process a single GCS JSON file into BigQuery."""
    if "mbta-dataset/" not in name:
        print(f"Skipping unrelated file: {name}")
        return

    print(f"Processing gs://{bucket_name}/{name}")
    try:
        table_name, route_hint = _infer_table_from_path(name)
        table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"

        blob = storage_client.bucket(bucket_name).blob(name)
        raw = blob.download_as_text()
        obj = json.loads(raw)
        rows = _flatten_mbta_json(obj, route_hint)

        if not rows:
            try:
                bq_client.get_table(table_id)
            except Exception:
                bq_client.create_table(bigquery.Table(table_id))
            print(f"Empty file; verified/created {table_id}")
            return

        try:
            bq_client.get_table(table_id)
        except Exception:
            sample = rows[0]
            schema = [bigquery.SchemaField(k, "STRING") for k in sample.keys()]
            table = bigquery.Table(table_id, schema=schema)
            bq_client.create_table(table)
            print(f"Created new table {table_id}")

        inserted = _insert_rows(table_id, rows)
        print(f"Inserted {inserted} rows into {table_id}")

    except Exception as e:
        print(f"Error processing file {name}: {e}")


# ---------------- TRIGGERED FUNCTION ----------------
def gcs_to_bq(event, context):
    """Triggered automatically when a new file appears in GCS."""
    bucket = event.get("bucket")
    name = event.get("name")

    if not bucket or not name:
        print("Missing bucket or name in event")
        return

    print(f"Triggered for gs://{bucket}/{name}")
    _process_single_file(bucket, name)


# ---------------- MANUAL ONE-TIME LOADER ----------------
def load_all_existing_from_gcs(request=None):
    """
    HTTP-triggered function (or callable from shell)
    Loads ALL existing files in ba882-team10-bucket/mbta-dataset into BigQuery.
    """
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = storage_client.list_blobs(bucket, prefix="mbta-dataset/")

    count = 0
    for blob in blobs:
        _process_single_file(BUCKET_NAME, blob.name)
        count += 1

    msg = f"Processed {count} existing files from {BUCKET_NAME}"
    print(msg)
    return msg
