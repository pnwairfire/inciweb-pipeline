"""CLI entrypoint: run Inciweb pipeline end-to-end and log to stdout.

python -m inciweb_pipeline <stream>
inciweb-pipeline <stream>
"""



import argparse
import logging
import sys

from inciweb_pipeline.incident_manager import IncidentManager
from inciweb_pipeline.payload_generator import PayloadGenerator
from inciweb_pipeline.db import STATEMENT_TIMEOUT, get_airfire_db_conn

logger = logging.getLogger(__name__)


def refresh_pm25():
    logger.info("BEGIN Refreshing underlying materialized views")
    airfire_conn = get_airfire_db_conn(STATEMENT_TIMEOUT)
    airfire_curr = airfire_conn.cursor()
    airfire_curr.execute("SELECT public.refresh_purple_air_hourly_measurements();")
    airfire_conn.commit()
    airfire_curr.close()
    logger.info("Completed refresh")


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
                    "status": "failed to generate data",
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
