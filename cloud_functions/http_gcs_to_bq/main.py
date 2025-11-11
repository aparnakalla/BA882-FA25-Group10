import json
import os
from typing import Dict, Any, List, Optional

from google.cloud import storage, bigquery

# --------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------
PROJECT_ID = os.environ.get("PROJECT_ID") or "christina-ba882-fall25"
BQ_DATASET = os.environ.get("BQ_DATASET") or "data_from_gcs_to_bq"
BUCKET_NAME_DEFAULT = os.environ.get("BUCKET_NAME") or "ba882-team10-bucket"
PREFIX_DEFAULT = os.environ.get("PREFIX") or "mbta-dataset/"

storage_client = storage.Client(project=PROJECT_ID)
bq_client = bigquery.Client(project=PROJECT_ID)


# --------------------------------------------------------------------
# HELPERS
# --------------------------------------------------------------------
def _flatten_mbta_json(obj: Dict[str, Any], route_hint: Optional[str]) -> List[Dict[str, Any]]:
    """
    MBTA v3 responses are shaped like:
      { "data": [ { "id": "...", "type": "...", "attributes": {...}, "relationships": {...} }, ... ] }

    We extract attributes and add useful ids. Also add route=... from the path if present.
    IMPORTANT: Any list/dict values are JSON-serialized so they fit into STRING columns in BigQuery.
    """
    data = obj.get("data", [])
    out: List[Dict[str, Any]] = []

    for item in data:
        rec: Dict[str, Any] = {}
        rec["id"] = item.get("id")
        rec["type"] = item.get("type")

        # attributes
        attrs = item.get("attributes", {}) or {}
        for k, v in attrs.items():
            # Convert complex types (list/dict) to JSON strings so they can be stored in STRING fields
            if isinstance(v, (dict, list)):
                rec[k] = json.dumps(v)
            else:
                rec[k] = v

        # relationships (pluck obvious ids if present)
        rels = item.get("relationships", {}) or {}
        for rel_name, rel_body in rels.items():
            rid = None
            if "data" in rel_body:
                rdata = rel_body["data"]
                if isinstance(rdata, dict):
                    rid = rdata.get("id")
                elif isinstance(rdata, list) and rdata:
                    rid = ",".join(
                        [str(x.get("id")) for x in rdata if isinstance(x, dict)]
                    )
            rec[f"{rel_name}_id"] = rid

        if route_hint:
            rec["route_hint"] = route_hint

        out.append(rec)

    return out


def _infer_table_from_path(gcs_name: str) -> (str, Optional[str]):
    """
    Expect paths like:
      mbta-dataset/routes/routes_20251109.json
      mbta-dataset/predictions/route=Red/predictions_20251109.json
    Returns (table_name, route_hint_or_none)
    """
    parts = gcs_name.split("/")
    if len(parts) < 2:
        raise ValueError(f"Unexpected object path: {gcs_name}")

    # second part is the endpoint folder: routes, predictions, trips, etc.
    table = parts[1]
    route_hint = None
    for p in parts:
        if p.startswith("route="):
            route_hint = p.split("=", 1)[1]
            break
    return table, route_hint


def _insert_rows(table_id: str, rows: List[Dict[str, Any]]) -> int:
    """
    Stream JSON rows into BigQuery.

    IMPORTANT: We LOG errors instead of raising, so one bad row
    does not crash the entire Cloud Function / Airflow DAG.
    """
    if not rows:
        return 0

    errors = bq_client.insert_rows_json(table_id, rows)
    if errors:
        # Log the errors but don't crash the function
        print(f"BigQuery insert errors for {table_id}: {errors[:3]}")
        # Count how many rows failed (best-effort)
        failed_indices = {e.get("index") for e in errors if "index" in e}
        successful = len(rows) - len(failed_indices)
        print(f"{successful} rows succeeded, {len(failed_indices)} rows failed for {table_id}")
        return successful

    return len(rows)


def _process_one_object(bucket: str, name: str) -> int:
    """
    Process a single GCS object and insert rows into BigQuery.
    """
    print(f"Processing gs://{bucket}/{name}")
    table_name, route_hint = _infer_table_from_path(name)
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"

    # Download object
    blob = storage_client.bucket(bucket).blob(name)
    raw = blob.download_as_text()
    obj = json.loads(raw)

    rows = _flatten_mbta_json(obj, route_hint)

    # Ensure table exists (create with inferred schema if needed)
    if not rows:
        try:
            bq_client.get_table(table_id)
        except Exception:
            bq_client.create_table(bigquery.Table(table_id))
        print(f"No rows in file {name}; created/verified table {table_id}")
        return 0

    try:
        bq_client.get_table(table_id)
    except Exception:
        sample = rows[0]
        schema = [bigquery.SchemaField(k, "STRING") for k in sample.keys()]
        table = bigquery.Table(table_id, schema=schema)
        bq_client.create_table(table)
        print(f"Created table {table_id} with inferred schema")

    inserted = _insert_rows(table_id, rows)
    print(f"Inserted {inserted} rows into {table_id} from {name}")
    return inserted


# --------------------------------------------------------------------
# ENTRYPOINT
# --------------------------------------------------------------------
def http_gcs_to_bq(request):
    """
    HTTP Cloud Function entry point.

    Modes:

    1) Bulk mode (your current Airflow DAG):
       - Airflow sends a simple GET with no body.
       - We use BUCKET_NAME and PREFIX env vars (or defaults) and
         scan all *.json files under that prefix.

    2) Single-object mode (optional):
       - Request body JSON: { "bucket": "...", "name": "mbta-dataset/routes/routes_20251109.json" }
       - Processes just that one file.

    Accepts GET and POST.
    """
    try:
        # Allow both GET and POST
        if request.method not in ("GET", "POST"):
            return (
                json.dumps({"error": "Only GET and POST are supported"}),
                405,
                {"Content-Type": "application/json"},
            )

        payload = request.get_json(silent=True) or {}
        bucket = payload.get("bucket") or BUCKET_NAME_DEFAULT
        name = payload.get("name")
        prefix = payload.get("prefix") or PREFIX_DEFAULT

        total_inserted = 0

        if name:
            # Single object mode
            total_inserted = _process_one_object(bucket, name)
        else:
            # Bulk mode: scan all objects under prefix
            print(f"Bulk load from bucket={bucket}, prefix={prefix}")
            bkt = storage_client.bucket(bucket)
            blobs = bkt.list_blobs(prefix=prefix)

            for blob in blobs:
                # Skip non-JSON artifacts
                if not blob.name.endswith(".json"):
                    continue
                total_inserted += _process_one_object(bucket, blob.name)

        return (
            json.dumps(
                {
                    "message": f"OK: inserted {total_inserted} rows from bucket={bucket}, prefix/name={name or prefix}"
                }
            ),
            200,
            {"Content-Type": "application/json"},
        )

    except Exception as e:
        print(f"Error in http_gcs_to_bq: {e}")
        return (
            json.dumps({"error": str(e)}),
            500,
            {"Content-Type": "application/json"},
        )

