"""CLI entrypoint: run Inciweb pipeline end-to-end and log to stdout.

python -m inciweb_pipeline <stream>
inciweb-pipeline <stream>
"""

import argparse
import logging
import sys

import psycopg2.errors
from inciweb_pipeline.constants import WIDGET_SCHEMA
from inciweb_pipeline.db import STATEMENT_TIMEOUT, get_airfire_db_conn
from inciweb_pipeline.incident_manager import IncidentManager
from inciweb_pipeline.payload_generator import PayloadGenerator

logger = logging.getLogger(__name__)


def refresh_pm25():
    logger.info("BEGIN Refreshing underlying materialized views")
    airfire_conn = get_airfire_db_conn(STATEMENT_TIMEOUT)
    airfire_curr = airfire_conn.cursor()

    queries_to_try = [
        f"SELECT {WIDGET_SCHEMA}.refresh_device_last_80_hourly_measurements();",
        "SELECT refresh_device_last_80_hourly_measurements();",
        f"SELECT {WIDGET_SCHEMA}.refresh_purple_air_hourly_measurements();",
        "SELECT refresh_purple_air_hourly_measurements();",
        "SELECT public.refresh_purple_air_hourly_measurements();",
    ]

    success = False
    for query in queries_to_try:
        try:
            airfire_curr.execute(query)
            airfire_conn.commit()
            logger.info(f"Successfully executed refresh via: {query}")
            success = True
            break
        except psycopg2.errors.UndefinedFunction:
            airfire_conn.rollback()
            airfire_curr = airfire_conn.cursor()
            continue
        except Exception as e:
            airfire_conn.rollback()
            airfire_curr = airfire_conn.cursor()
            logger.warning(f"Error trying query '{query}': {e}")
            continue

    airfire_curr.close()
    if success:
        logger.info("Completed refresh")
    else:
        logger.warning(
            "Materialized view refresh function not found in database; "
            "skipping refresh step and proceeding with incident extraction."
        )


def get_incident_rows() -> list:
    im = IncidentManager()
    im.get_incidents()
    rows = im.to_rows()
    logger.info(f"EXTRACTED {len(rows)} Incidents")
    return rows


def generate_payloads(rows):
    results = []
    for row in rows:
        inciweb_id = row[0]
        logger.info(f"TRANSFORMING InciwebID: {inciweb_id}")
        try:
            pg = PayloadGenerator(row)
            pg.generate_and_write_to_s3()
            results.append({"id": inciweb_id, "status": "success"})
        except Exception as e:
            logger.error(f"Failed to generate payload for id {inciweb_id}: {e}")
            results.append(
                {
                    "id": inciweb_id,
                    "status": (
                        "failed to generate data -- could be due to no AQ observations"
                    ),
                }
            )
    return results


def inciweb_chart_data_ingest():
    refresh_pm25()
    rows = get_incident_rows()
    results = generate_payloads(rows)
    successes = sum(1 for r in results if r["status"] == "success")
    return f"processed {len(results)} incidents ({successes} successful)"


def run():
    return inciweb_chart_data_ingest()


REGISTRY = {
    "inciweb-chart-data-ingest": inciweb_chart_data_ingest,
}


def main(argv=None):
    parser = argparse.ArgumentParser(prog="inciweb-pipeline", description=__doc__)
    parser.add_argument(
        "stream", choices=sorted(REGISTRY), help="which pipeline to run"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="stdlib logging level (default: INFO)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    func = REGISTRY[args.stream]
    logger.info(f"Starting pipeline: {args.stream}")
    summary = func()
    logger.info(f"Finished pipeline: {args.stream} — {summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
