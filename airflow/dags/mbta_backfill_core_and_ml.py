# dags/mbta_backfill_core_and_ml.py

from __future__ import annotations

from datetime import timedelta
import os

import pendulum
from airflow.decorators import dag
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

TIMEZONE = "America/New_York"

PROJECT_ID = os.environ.get("PROJECT_ID", "christina-ba882-fall25")
BQ_DATASET = os.environ.get("BQ_DATASET", "data_from_gcs_to_bq")
CORE_DATASET = os.environ.get("CORE_DATASET", "mbta_core")
ML_DATASET = os.environ.get("ML_DATASET", "mbta_ml")


@dag(
    dag_id="mbta_backfill_core_and_ml",
    start_date=pendulum.datetime(2025, 11, 1, tz=TIMEZONE),
    schedule=None,          # MANUAL trigger only
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["mbta", "backfill", "core", "ml"],
)
def mbta_backfill_core_and_ml():
    """
    One-shot backfill pipeline:
      raw -> mbta_core.* (dims, facts) -> fact_delay_stop -> marts -> mbta_ml.training_delay_stop

    This DAG is intended to be triggered manually to rebuild everything from
    ALL history in data_from_gcs_to_bq.* (no date filters). Your existing
    daily / incremental DAGs are left untouched.
    """

    # ----------------------------------------------------------------------
    # DIMENSIONS (no date filters; full history)
    # ----------------------------------------------------------------------

    build_dim_line = BigQueryInsertJobOperator(
        task_id="build_dim_line",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.dim_line` AS
                    SELECT DISTINCT
                      id          AS line_id,
                      type        AS line_type,
                      long_name   AS line_long_name,
                      short_name  AS line_short_name,
                      color,
                      text_color,
                      sort_order,
                      DATE(ingest_ts) AS snapshot_date,
                      ingest_ts
                    FROM `{PROJECT_ID}.{BQ_DATASET}.lines`;
                """,
                "useLegacySql": False,
            }
        },
    )

    build_dim_route = BigQueryInsertJobOperator(
        task_id="build_dim_route",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.dim_route` AS
                    SELECT DISTINCT
                      id          AS route_id,
                      line_id,
                      fare_class,
                      listed_route,
                      sort_order,
                      DATE(ingest_ts) AS snapshot_date,
                      ingest_ts
                    FROM `{PROJECT_ID}.{BQ_DATASET}.routes`;
                """,
                "useLegacySql": False,
            }
        },
    )

    build_dim_stop = BigQueryInsertJobOperator(
        task_id="build_dim_stop",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.dim_stop` AS
                    SELECT DISTINCT
                      id          AS stop_id,
                      name        AS stop_name,
                      municipality,
                      latitude,
                      longitude,
                      zone_id,
                      parent_station_id,
                      platform_name,
                      vehicle_type,
                      DATE(ingest_ts) AS snapshot_date,
                      ingest_ts
                    FROM `{PROJECT_ID}.{BQ_DATASET}.stops`;
                """,
                "useLegacySql": False,
            }
        },
    )

    build_dim_route_pattern = BigQueryInsertJobOperator(
        task_id="build_dim_route_pattern",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.dim_route_pattern` AS
                    SELECT DISTINCT
                      id                  AS route_pattern_id,
                      route_id,
                      direction_id,
                      name,
                      canonical,
                      typicality,
                      time_desc,
                      representative_trip_id,
                      DATE(ingest_ts) AS snapshot_date,
                      ingest_ts
                    FROM `{PROJECT_ID}.{BQ_DATASET}.route_patterns`;
                """,
                "useLegacySql": False,
            }
        },
    )

    build_dim_shape = BigQueryInsertJobOperator(
        task_id="build_dim_shape",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.dim_shape` AS
                    SELECT DISTINCT
                      id          AS shape_id,
                      polyline,
                      route_hint,
                      DATE(ingest_ts) AS snapshot_date,
                      ingest_ts
                    FROM `{PROJECT_ID}.{BQ_DATASET}.shapes`;
                """,
                "useLegacySql": False,
            }
        },
    )

    build_dim_trip = BigQueryInsertJobOperator(
        task_id="build_dim_trip",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.dim_trip` AS
                    SELECT DISTINCT
                      id             AS trip_id,
                      route_id,
                      direction_id,
                      headsign,
                      service_id,
                      route_pattern_id,
                      shape_id,
                      bikes_allowed,
                      wheelchair_accessible,
                      DATE(ingest_ts) AS snapshot_date,
                      ingest_ts
                    FROM `{PROJECT_ID}.{BQ_DATASET}.trips`;
                """,
                "useLegacySql": False,
            }
        },
    )

    build_dim_facility = BigQueryInsertJobOperator(
        task_id="build_dim_facility",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.dim_facility` AS
                    SELECT DISTINCT
                      id          AS facility_id,
                      type        AS facility_type,
                      stop_id,
                      short_name,
                      long_name,
                      latitude,
                      longitude,
                      properties,
                      DATE(ingest_ts) AS snapshot_date,
                      ingest_ts
                    FROM `{PROJECT_ID}.{BQ_DATASET}.facilities`;
                """,
                "useLegacySql": False,
            }
        },
    )

    core_dims = [
        build_dim_line,
        build_dim_route,
        build_dim_stop,
        build_dim_route_pattern,
        build_dim_shape,
        build_dim_trip,
        build_dim_facility,
    ]

    # ----------------------------------------------------------------------
    # FACTS (no date filters; dedupe with ROW_NUMBER)
    # ----------------------------------------------------------------------

    build_fact_schedule_stop = BigQueryInsertJobOperator(
        task_id="build_fact_schedule_stop",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.fact_schedule_stop` AS
                    WITH latest AS (
                      SELECT
                        id AS schedule_id,
                        trip_id,
                        route_id,
                        stop_id,
                        direction_id,
                        stop_sequence,
                        arrival_time   AS arrival_time_scheduled,
                        departure_time AS departure_time_scheduled,
                        DATE(ingest_ts) AS snapshot_date,
                        ingest_ts,
                        ROW_NUMBER() OVER (
                          PARTITION BY id
                          ORDER BY ingest_ts DESC
                        ) AS rn
                      FROM `{PROJECT_ID}.{BQ_DATASET}.schedules`
                    )
                    SELECT
                      schedule_id,
                      trip_id,
                      route_id,
                      stop_id,
                      direction_id,
                      stop_sequence,
                      arrival_time_scheduled,
                      departure_time_scheduled,
                      snapshot_date,
                      ingest_ts
                    FROM latest
                    WHERE rn = 1;
                """,
                "useLegacySql": False,
            }
        },
    )

    build_fact_prediction_stop = BigQueryInsertJobOperator(
        task_id="build_fact_prediction_stop",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.fact_prediction_stop` AS
                    WITH latest AS (
                      SELECT
                        id AS prediction_id,
                        TIMESTAMP(ingest_ts) AS prediction_ts,
                        trip_id,
                        route_id,
                        stop_id,
                        stop_sequence,
                        direction_id,
                        arrival_time   AS arrival_time_predicted,
                        departure_time AS departure_time_predicted,
                        schedule_relationship,
                        status,
                        update_type,
                        revenue,
                        route_hint,
                        DATE(ingest_ts) AS snapshot_date,
                        ingest_ts,
                        ROW_NUMBER() OVER (
                          PARTITION BY id
                          ORDER BY ingest_ts DESC
                        ) AS rn
                      FROM `{PROJECT_ID}.{BQ_DATASET}.predictions`
                    )
                    SELECT
                      prediction_id,
                      prediction_ts,
                      trip_id,
                      route_id,
                      stop_id,
                      stop_sequence,
                      direction_id,
                      arrival_time_predicted,
                      departure_time_predicted,
                      schedule_relationship,
                      status,
                      update_type,
                      revenue,
                      route_hint,
                      snapshot_date,
                      ingest_ts
                    FROM latest
                    WHERE rn = 1;
                """,
                "useLegacySql": False,
            }
        },
    )

    build_fact_vehicle_position = BigQueryInsertJobOperator(
        task_id="build_fact_vehicle_position",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.fact_vehicle_position` AS
                    WITH latest AS (
                      SELECT
                        id AS vehicle_id,
                        TIMESTAMP(updated_at) AS event_ts,
                        trip_id,
                        route_id,
                        stop_id,
                        direction_id,
                        latitude,
                        longitude,
                        speed,
                        bearing,
                        current_status,
                        current_stop_sequence,
                        occupancy_status,
                        revenue,
                        label,
                        DATE(ingest_ts) AS snapshot_date,
                        ingest_ts,
                        ROW_NUMBER() OVER (
                          PARTITION BY id, updated_at
                          ORDER BY ingest_ts DESC
                        ) AS rn
                      FROM `{PROJECT_ID}.{BQ_DATASET}.vehicles`
                    )
                    SELECT
                      vehicle_id,
                      event_ts,
                      trip_id,
                      route_id,
                      stop_id,
                      direction_id,
                      latitude,
                      longitude,
                      speed,
                      bearing,
                      current_status,
                      current_stop_sequence,
                      occupancy_status,
                      revenue,
                      label,
                      snapshot_date,
                      ingest_ts
                    FROM latest
                    WHERE rn = 1;
                """,
                "useLegacySql": False,
            }
        },
    )

    build_fact_alert = BigQueryInsertJobOperator(
        task_id="build_fact_alert",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.fact_alert` AS
                    WITH latest AS (
                      SELECT
                        id AS alert_id,
                        type,
                        cause,
                        effect,
                        severity,
                        lifecycle,
                        timeframe,
                        created_at,
                        updated_at,
                        short_header,
                        header,
                        description,
                        service_effect,
                        active_period,
                        informed_entity,
                        closed_timestamp,
                        duration_certainty,
                        DATE(ingest_ts) AS snapshot_date,
                        ingest_ts,
                        ROW_NUMBER() OVER (
                          PARTITION BY id
                          ORDER BY ingest_ts DESC
                        ) AS rn
                      FROM `{PROJECT_ID}.{BQ_DATASET}.alerts`
                    )
                    SELECT
                      alert_id,
                      type,
                      cause,
                      effect,
                      severity,
                      lifecycle,
                      timeframe,
                      created_at,
                      updated_at,
                      short_header,
                      header,
                      description,
                      service_effect,
                      active_period,
                      informed_entity,
                      closed_timestamp,
                      duration_certainty,
                      snapshot_date,
                      ingest_ts
                    FROM latest
                    WHERE rn = 1;
                """,
                "useLegacySql": False,
            }
        },
    )

    core_facts = [
        build_fact_schedule_stop,
        build_fact_prediction_stop,
        build_fact_vehicle_position,
        build_fact_alert,
    ]

    # ----------------------------------------------------------------------
    # DELAY FACT
    # ----------------------------------------------------------------------

    build_fact_delay_stop = BigQueryInsertJobOperator(
        task_id="build_fact_delay_stop",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.fact_delay_stop` AS
                    WITH joined AS (
                      SELECT
                        p.prediction_id,
                        p.prediction_ts,
                        p.route_id,
                        p.trip_id,
                        p.stop_id,
                        p.stop_sequence,
                        p.direction_id,
                        s.arrival_time_scheduled,
                        p.arrival_time_predicted,
                        p.snapshot_date,
                        p.ingest_ts
                      FROM `{PROJECT_ID}.{CORE_DATASET}.fact_prediction_stop` p
                      LEFT JOIN `{PROJECT_ID}.{CORE_DATASET}.fact_schedule_stop` s
                        USING (trip_id, route_id, stop_id, stop_sequence, direction_id, snapshot_date)
                    )
                    SELECT
                      DATE(prediction_ts) AS service_date,
                      route_id,
                      trip_id,
                      stop_id,
                      direction_id,
                      stop_sequence,
                      prediction_ts,
                      arrival_time_scheduled,
                      arrival_time_predicted,
                      TIMESTAMP_DIFF(
                        TIMESTAMP(arrival_time_predicted),
                        TIMESTAMP(arrival_time_scheduled),
                        SECOND
                      ) AS delay_seconds,
                      CASE
                        WHEN TIMESTAMP_DIFF(
                          TIMESTAMP(arrival_time_predicted),
                          TIMESTAMP(arrival_time_scheduled),
                          SECOND
                        ) > 300 THEN 1 ELSE 0
                      END AS is_delayed_5min,
                      DATE(ingest_ts) AS snapshot_date,
                      ingest_ts
                    FROM joined
                    WHERE arrival_time_scheduled IS NOT NULL
                      AND arrival_time_predicted IS NOT NULL;
                """,
                "useLegacySql": False,
            }
        },
    )

    # ----------------------------------------------------------------------
    # MART TABLES
    # ----------------------------------------------------------------------

    mart_route_day_delay = BigQueryInsertJobOperator(
        task_id="mart_route_day_delay",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.mart_route_day_delay` AS
                    SELECT
                      d.service_date,
                      d.route_id,
                      r.line_id,
                      r.listed_route,
                      COUNT(*) AS n_predictions,
                      AVG(delay_seconds) AS avg_delay_seconds,
                      APPROX_QUANTILES(delay_seconds, 101)[OFFSET(50)] AS p50_delay_seconds,
                      APPROX_QUANTILES(delay_seconds, 101)[OFFSET(90)] AS p90_delay_seconds,
                      AVG(CASE WHEN is_delayed_5min = 1 THEN 1 ELSE 0 END) AS pct_trips_delayed_5min
                    FROM `{PROJECT_ID}.{CORE_DATASET}.fact_delay_stop` d
                    LEFT JOIN `{PROJECT_ID}.{CORE_DATASET}.dim_route` r
                      USING (route_id, snapshot_date)
                    GROUP BY 1, 2, 3, 4;
                """,
                "useLegacySql": False,
            }
        },
    )

    mart_stop_delay = BigQueryInsertJobOperator(
        task_id="mart_stop_delay",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.mart_stop_delay` AS
                    SELECT
                      d.service_date,
                      d.route_id,
                      d.stop_id,
                      s.stop_name,
                      d.direction_id,
                      COUNT(*) AS n_predictions,
                      AVG(delay_seconds) AS avg_delay_seconds,
                      AVG(CASE WHEN is_delayed_5min = 1 THEN 1 ELSE 0 END) AS pct_delayed_5min
                    FROM `{PROJECT_ID}.{CORE_DATASET}.fact_delay_stop` d
                    LEFT JOIN `{PROJECT_ID}.{CORE_DATASET}.dim_stop` s
                      USING (stop_id, snapshot_date)
                    GROUP BY 1, 2, 3, 4, 5;
                """,
                "useLegacySql": False,
            }
        },
    )

    mart_line_hour_delay = BigQueryInsertJobOperator(
        task_id="mart_line_hour_delay",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{CORE_DATASET}.mart_line_hour_delay` AS
                    SELECT
                      d.service_date,
                      r.line_id,
                      EXTRACT(HOUR FROM d.prediction_ts) AS hour_of_day,
                      COUNT(*) AS n_predictions,
                      AVG(delay_seconds) AS avg_delay_seconds,
                      AVG(CASE WHEN is_delayed_5min = 1 THEN 1 ELSE 0 END) AS pct_delayed_5min
                    FROM `{PROJECT_ID}.{CORE_DATASET}.fact_delay_stop` d
                    LEFT JOIN `{PROJECT_ID}.{CORE_DATASET}.dim_route` r
                      USING (route_id, snapshot_date)
                    GROUP BY 1, 2, 3;
                """,
                "useLegacySql": False,
            }
        },
    )

    # ----------------------------------------------------------------------
    # ML TRAINING TABLE
    # ----------------------------------------------------------------------

    build_ml_training_delay_stop = BigQueryInsertJobOperator(
        task_id="build_ml_training_delay_stop",
        configuration={
            "query": {
                "query": f"""
                    CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{ML_DATASET}`;

                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{ML_DATASET}.training_delay_stop` AS
                    SELECT
                      d.service_date,
                      d.route_id,
                      r.line_id,
                      r.listed_route,
                      d.trip_id,
                      d.stop_id,
                      s.stop_name,
                      d.direction_id,
                      d.stop_sequence,
                      d.prediction_ts,
                      EXTRACT(HOUR FROM d.prediction_ts)      AS hour_of_day,
                      EXTRACT(DAYOFWEEK FROM d.prediction_ts) AS day_of_week,
                      IF(EXTRACT(DAYOFWEEK FROM d.prediction_ts) IN (1,7), 1, 0) AS is_weekend,
                      d.delay_seconds,
                      d.is_delayed_5min AS label_is_delayed_5min
                    FROM `{PROJECT_ID}.{CORE_DATASET}.fact_delay_stop` d
                    LEFT JOIN `{PROJECT_ID}.{CORE_DATASET}.dim_route` r
                      USING (route_id, snapshot_date)
                    LEFT JOIN `{PROJECT_ID}.{CORE_DATASET}.dim_stop` s
                      USING (stop_id, snapshot_date)
                    WHERE d.delay_seconds IS NOT NULL;
                """,
                "useLegacySql": False,
            }
        },
    )

    # ----------------------------------------------------------------------
    # DEPENDENCIES
    # ----------------------------------------------------------------------

    # dims -> facts
    for f in core_facts:
        core_dims >> f

    # facts -> delay fact
    core_facts >> build_fact_delay_stop

    # delay fact -> marts + training table
    build_fact_delay_stop >> [mart_route_day_delay, mart_stop_delay, mart_line_hour_delay, build_ml_training_delay_stop]


mbta_backfill_core_and_ml_dag = mbta_backfill_core_and_ml()
