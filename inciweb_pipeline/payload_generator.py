import json
import logging
from datetime import datetime, timedelta

import pandas as pd

from inciweb_pipeline.near_me_util import fetch_near_me
from inciweb_pipeline.aqi_util import count_cat_values, pm25_to_aqi
from inciweb_pipeline.db import STATEMENT_TIMEOUT, get_airfire_db_conn
from inciweb_pipeline.s3 import inciweb_bucket, init_s3


class PayloadGenerator:
    """
    Creates and formats payload data given a row of incident data

    currently incident data is defined in incident_manager.py

    in index order
    - inciweb_id
    - lon
    - lat
    - acres
    - fasm_tracking
    - itype
    - distances
    - fasm_fire_last_updated
    - nearby_wildfire
    - nearby_rx
    """

    def __init__(self, incident_row=None):
        self.payload = {}
        self.near_me_data = None
        self.logger = logging.getLogger(__name__)

        if incident_row:
            self.inciweb_id = incident_row[0]
            self.lon = incident_row[1]
            self.lat = incident_row[2]
            self.acres = incident_row[3]
            self.fasm_tracking = incident_row[4]
            self.itype = incident_row[5]
            self.distances = incident_row[6]
            self.fasm_fire_last_updated = incident_row[7]
            self.nearby_wildfire = incident_row[8]
            self.nearby_rx = incident_row[9]
            self._set_payload_distances()
        else:
            print("Empty payload generated -- NO INCIDENT INFO PROVIDED")

    def generate(self):
        self._get_last_72_pm25()
        self._get_last_72_pm25_cat()
        self._get_population_data()
        self._get_near_me_data()
        self._set_meta()

    def generate_and_write_to_s3(self):
        self.generate()
        self._write_to_s3()

    def _set_payload_distances(self):
        for dist in self.distances:
            self.payload[dist] = {
                "population": None,
                "monitors": None,
                "sensors": None,
                "last_72": None,
            }

    def _get_population_data(self):
        """
        Maybe we should not query the DB everytime for this?
        Easier to re-use last payload?
        """
        airfire_conn = get_airfire_db_conn(STATEMENT_TIMEOUT)
        airfire_curr = airfire_conn.cursor()

        for dist in self.distances:
            airfire_curr.execute(
                "SELECT population_in_box(%s, %s, %s);",
                (self.lat, self.lon, (dist * 1.609344)),
            )
            population = airfire_curr.fetchone()[0]
            self.payload[dist]["population"] = population

        airfire_curr.close()
        airfire_conn.close()

    def _get_last_72_pm25(self):
        # range chart
        airfire_conn = get_airfire_db_conn(STATEMENT_TIMEOUT)
        airfire_curr = airfire_conn.cursor()

        for dist in self.distances:
            airfire_curr.execute(
                "SELECT pm25_values_in_box(%s, %s, %s);",
                (self.lat, self.lon, (dist * 1.609344)),
            )
            results = airfire_curr.fetchall()
            timestamps, values = self._format_last_72_results(results)

            if not timestamps or not values:
                self.logger.info(
                    f"No last 72 for: {self.inciweb_id} at distance: {dist}"
                )

            self.payload[dist]["last_72"] = {"timestamps": timestamps, "values": values}

        airfire_curr.close()
        airfire_conn.close()

    def _get_last_72_pm25_cat(self):
        # bar chart
        airfire_conn = get_airfire_db_conn(STATEMENT_TIMEOUT)
        airfire_curr = airfire_conn.cursor()

        for dist in self.distances:
            airfire_curr.execute(
                "SELECT aqi_cats_in_box(%s, %s, %s);",
                (self.lat, self.lon, (dist * 1.609344)),
            )
            results = airfire_curr.fetchall()
            timestamps, values = self._format_last_72_cat_results(results)

            if not timestamps or not values:
                self.logger.info(
                    f"No last 72 CATS for: {self.inciweb_id} at distance: {dist}"
                )

            self.payload[dist]["last_72_cat"] = {
                "timestamps": timestamps,
                "values": values,
            }

        airfire_curr.close()
        airfire_conn.close()

    def _get_near_me_data(self):
        for dist in self.distances:
            self.near_me_data = fetch_near_me(self.lat, self.lon, dist)

            if self.near_me_data:
                self.payload[dist]["monitors"] = [
                    pm25_to_aqi(monitor["nowcast"])
                    for monitor in self.near_me_data.get("aqMonitors", [])
                    if monitor.get("nowcast") is not None
                    and monitor.get("device_type", "").lower() != "sensor"
                ]
                self.payload[dist]["monitors"] = count_cat_values(
                    self.payload[dist]["monitors"]
                )

                all_sensors = (
                    self.near_me_data.get("purpleAir", [])
                    + self.near_me_data.get("clarity", [])
                    + [
                        monitor
                        for monitor in self.near_me_data.get("aqMonitors", [])
                        if monitor.get("device_type", "").lower() == "sensor"
                    ]
                )
                self.payload[dist]["sensors"] = [
                    sensor["aqi"]
                    for sensor in all_sensors
                    if sensor.get("aqi") is not None
                ]
                self.payload[dist]["sensors"] = count_cat_values(
                    self.payload[dist]["sensors"]
                )

                self.payload[dist]["outlooks"] = self.near_me_data.get("outlooks")

                # Number of HMS Fire Detections
                self.payload[dist]["count_hms_detects"] = len(
                    self.near_me_data.get("fires")
                )

                # Nearby fasmFireIncidents
                keys = [
                    "fasmFireId",
                    "lat",
                    "lng",
                    "incidentName",
                    "fireType",
                    "cumulativeAcres",
                    "inciwebUrl",
                    "distanceMiles",
                    "startTime",
                    "lastUpdated",
                ]
                self.payload[dist]["nearby_incidents"] = [
                    {k: d.get(k) for k in keys}
                    for d in self.near_me_data.get("fasmFireIncidents", [])
                ]

    def _set_meta(self):
        if not self.near_me_data:
            raise ValueError("self.near_me_data must be set before trying to set meta")

        all_aq_units = (
            self.near_me_data.get("purpleAir", [])
            + self.near_me_data.get("clarity", [])
            + self.near_me_data.get("aqMonitors", [])
        )

        if all_aq_units:
            dt = datetime.fromisoformat(max(item["utc_ts"] for item in all_aq_units))
            current_as_of = str(int(dt.timestamp()))
        else:
            current_as_of = ""

        # pd.Timestamp is not JSON-serializable; convert to ISO string or None
        if pd.notna(self.fasm_fire_last_updated):
            fasm_last_updated = self.fasm_fire_last_updated.isoformat()
        else:
            fasm_last_updated = None

        self.payload["meta"] = {
            "current_as_of": current_as_of,
            "distance_keys": self.distances,
            "fasm_tracking": self.fasm_tracking,
            "fasm_fire_last_updated": fasm_last_updated,
            "coords": [self.lat, self.lon],
            "fire_itype": self.itype,
            "nearby_wildfire": self.nearby_wildfire,
            "nearby_rx": self.nearby_rx,
        }

    def _format_last_72_results(self, results):
        timestamps = []
        values = []

        for row in results:
            try:
                # Extract the string from the tuple and remove parentheses
                data_string = row[0].strip("()")

                # Split by comma and clean up quotes
                parts = [part.strip(' "') for part in data_string.split(",")]

                timestamp_str = parts[0]
                max_val = float(parts[1]) if parts[1] else None
                min_val = float(parts[2]) if parts[2] else None
                avg_val = float(parts[3]) if parts[3] else None

                # Convert timestamp string to Unix timestamp (seconds)
                dt = datetime.fromisoformat(
                    timestamp_str.replace(" ", "T") + "+00:00"
                )  # Assume UTC
                unix_timestamp = int(dt.timestamp())

                timestamps.append(unix_timestamp)
                values.append([max_val, min_val, avg_val])
            except Exception as e:
                self.logger.error(e)
                self.logger.error(f"failed on {row}")

        return timestamps, values

    def _format_last_72_cat_results(self, results):
        timestamps = []
        values = []
        for row in results:
            try:
                # Extract the string from the tuple and remove parentheses
                data_string = row[0].strip("()")

                # Split by comma and clean up quotes
                parts = [part.strip(' "') for part in data_string.split(",")]

                timestamp_str = parts[0]
                cat_0 = float(parts[1]) if parts[1] else None
                cat_1 = float(parts[2]) if parts[2] else None
                cat_2 = float(parts[3]) if parts[3] else None
                cat_3 = float(parts[4]) if parts[4] else None
                cat_4 = float(parts[5]) if parts[5] else None
                cat_5 = (
                    float(parts[6]) if len(parts) > 6 and parts[6] else None
                )

                # Convert timestamp string to Unix timestamp (seconds)
                dt = datetime.fromisoformat(
                    timestamp_str.replace(" ", "T") + "+00:00"
                )  # Assume UTC
                unix_timestamp = int(dt.timestamp())

                timestamps.append(unix_timestamp)
                values.append([cat_0, cat_1, cat_2, cat_3, cat_4, cat_5])
            except Exception as e:
                self.logger.error(e)
                self.logger.error(f"failed on {row}")
        return timestamps, values

    def _write_to_s3(self):
        expiration_date = datetime.utcnow() + timedelta(days=30)
        s3 = init_s3()
        s3.put_object(
            Bucket=inciweb_bucket(),
            Key=f"{self.inciweb_id}/latest/payload.json",
            Body=json.dumps(self.payload),
            Expires=expiration_date,
            ContentType="application/json",
        )
