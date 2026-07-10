import pandas as pd
import psycopg2

from inciweb_pipeline.db import airfire_pg_uri
from inciweb_pipeline.sql_util import read_sql

# thresholds for incident size
WILDFIRE_SIZE_THRESHOLD = 10000

# distances to consider per incident type and size
LARGE_WILDFIRE_DIST = [50, 75, 100]
SMALL_WILDFIRE_DIST = [25, 50, 75]
RX_FIRE_DIST = [25, 35, 50]


class IncidentManager:
    """Manages inciweb data"""

    def __init__(self):
        self.uri = airfire_pg_uri()

    def get_incidents(self):
        """
        Query fire info DB and return list of incidents.

        Returns:
            df: df of current incidents
        """
        query = read_sql("query_fasm_inciweb.sql")

        conn = psycopg2.connect(self.uri)
        df = pd.read_sql_query(query, conn)
        conn.close()

        # psycopg2 returns pg timestamps as datetime but pd.read_sql may
        # leave them as object when every value is NULL — force the dtype.
        df["fasm_fire_last_updated"] = pd.to_datetime(
            df["fasm_fire_last_updated"], utc=True, errors="coerce"
        )

        # psycopg2 auto-parses pg json columns to Python lists/dicts,
        # but COALESCE may return a string '[]' — normalise to list.
        for col in ("nearby_wildfire", "nearby_rx"):
            df[col] = df[col].apply(
                lambda v: (
                    v if isinstance(v, list) else [] if v is None or v == "[]" else v
                )
            )

        self.current_incidents = df

        if not self.current_incidents.empty:
            self._set_distances()

        return df

    def to_rows(self):
        if (
            not hasattr(self, "current_incidents")
            or self.current_incidents is None
            or self.current_incidents.empty
        ):
            raise ValueError(
                "No incidents available. Call get_incidents() "
                "or check error logs first."
            )

        rows = list(
            self.current_incidents[
                [
                    "inciweb_id",
                    "lon",
                    "lat",
                    "acres",
                    "fasm_tracking",
                    "itype",
                    "distances",
                    "fasm_fire_last_updated",
                    "nearby_wildfire",
                    "nearby_rx",
                ]
            ].itertuples(index=False, name=None)
        )

        return rows

    def _set_distances(self):
        """Set distances based on incident type and size."""

        def get_distance_list(row):
            itype = row["itype"].lower()
            acres = row["acres"]

            if itype == "wildfire":
                if acres > WILDFIRE_SIZE_THRESHOLD:
                    return LARGE_WILDFIRE_DIST
                else:
                    return SMALL_WILDFIRE_DIST
            elif "prescribed" in itype:
                return RX_FIRE_DIST
            else:
                return SMALL_WILDFIRE_DIST

        self.current_incidents["distances"] = self.current_incidents.apply(
            get_distance_list, axis=1
        )
