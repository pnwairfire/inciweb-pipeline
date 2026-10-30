from unittest.mock import MagicMock, patch

from inciweb_pipeline.__main__ import generate_payloads, get_incident_rows, refresh_pm25
from inciweb_pipeline.constants import WIDGET_SCHEMA
from inciweb_pipeline.db import REFRESH_TIMEOUT
from inciweb_pipeline.payload_generator import PayloadGenerator


def test_refresh_pm25():
    mock_conn = MagicMock()
    mock_curr = MagicMock()
    mock_conn.cursor.return_value = mock_curr

    with patch(
        "inciweb_pipeline.__main__.get_airfire_db_conn", return_value=mock_conn
    ) as mock_get_conn:
        refresh_pm25()

    mock_get_conn.assert_called_once_with(REFRESH_TIMEOUT)
    mock_curr.execute.assert_called_once_with(
        f"SELECT {WIDGET_SCHEMA}.refresh_device_last_80_hourly_measurements();"
    )
    mock_conn.commit.assert_called_once()
    mock_curr.close.assert_called_once()
    mock_conn.close.assert_called_once()


def test_get_incident_rows():
    mock_im = MagicMock()
    mock_im.to_rows.return_value = [["inc_1", -120.0, 45.0]]

    with patch("inciweb_pipeline.__main__.IncidentManager", return_value=mock_im):
        rows = get_incident_rows()

    assert rows == [["inc_1", -120.0, 45.0]]
    mock_im.get_incidents.assert_called_once()


def test_generate_payloads_success_and_failure():
    rows = [["inc_101"], ["inc_102"]]

    def mock_pg_init(row, **kwargs):
        mock_pg = MagicMock()
        if row[0] == "inc_102":
            mock_pg.generate_and_write_to_s3.side_effect = Exception("No AQ obs")
        return mock_pg

    with patch("inciweb_pipeline.__main__.PayloadGenerator", side_effect=mock_pg_init):
        results = generate_payloads(rows)

    assert len(results) == 2
    assert results[0] == {"id": "inc_101", "status": "success"}
    assert results[1] == {
        "id": "inc_102",
        "status": "failed to generate data -- could be due to no AQ observations",
    }


def test_inciweb_chart_data_ingest_zero_success_raises():
    import pytest
    from inciweb_pipeline.__main__ import inciweb_chart_data_ingest

    with (
        patch("inciweb_pipeline.__main__.refresh_pm25"),
        patch("inciweb_pipeline.__main__.get_incident_rows", return_value=[["inc_1"]]),
        patch(
            "inciweb_pipeline.__main__.generate_payloads",
            return_value=[{"id": "inc_1", "status": "failed"}],
        ),
    ):
        with pytest.raises(RuntimeError, match="0 of 1 incidents were successful"):
            inciweb_chart_data_ingest()


def test_inciweb_bucket_dynamic_lookup():
    import os
    from inciweb_pipeline.s3 import inciweb_bucket

    with patch.dict(os.environ, {"INCIWEB_BUCKET": "dynamic-inciweb-bucket"}):
        assert inciweb_bucket() == "dynamic-inciweb-bucket"


def test_set_meta_handles_invalid_date():
    from inciweb_pipeline.payload_generator import PayloadGenerator

    pg = PayloadGenerator(
        incident_row=[
            "inc_test",
            -120.0,
            45.0,
            100,
            True,
            "WF",
            [10, 25],
            "2026-08-22T00:00:00",
            True,
            False,
        ]
    )
    pg.near_me_data = {
        "purpleAir": [{"utc_ts": "Invalid Date"}, {"utc_ts": None}],
        "clarity": [{"utc_ts": "2026-08-22T12:00:00Z"}],
        "aqMonitors": [{"utc_ts": "malformed_ts"}],
    }
    pg._set_meta()
    assert "meta" in pg.payload
    # Should use the single valid timestamp (2026-08-22T12:00:00Z)
    assert pg.payload["meta"]["current_as_of"] != ""


def test_payload_generator_parity_no_dead_last_72_pm25():
    from inciweb_pipeline.payload_generator import PayloadGenerator

    assert not hasattr(PayloadGenerator, "_get_last_72_pm25")
    assert not hasattr(PayloadGenerator, "_format_last_72_results")

    mock_conn = MagicMock()
    mock_curr = MagicMock()
    mock_conn.cursor.return_value = mock_curr
    mock_curr.fetchall.return_value = [('("2026-08-22 12:00:00",1,2,0,0,0,0,3)',)]
    mock_curr.fetchone.return_value = (1500,)

    pg = PayloadGenerator(
        incident_row=[
            "inc_parity",
            -120.0,
            45.0,
            100,
            True,
            "WF",
            [10],
            "2026-08-22T00:00:00",
            True,
            False,
        ],
        db_conn=mock_conn,
    )

    near_me_sample = {
        "purpleAir": [],
        "clarity": [{"utc_ts": "2026-08-22T12:00:00Z", "aqi": 42}],
        "aqMonitors": [],
        "outlooks": [],
        "fires": [],
        "fasmFireIncidents": [],
    }
    with patch(
        "inciweb_pipeline.payload_generator.fetch_near_me",
        return_value=near_me_sample,
    ):
        pg.generate()

    assert pg.payload[10]["last_72"] is None
    assert "last_72_cat" in pg.payload[10]
    assert pg.payload[10]["last_72_cat"]["values"] == [[1.0, 2.0, 0.0, 0.0, 0.0, 0.0]]
    assert pg.payload[10]["population"] == 1500


def test_sql_ddl_files_present():
    from inciweb_pipeline.sql_util import read_sql

    functions_sql = read_sql("functions.sql")
    views_sql = read_sql("views_and_materialized_views.sql")

    assert "inciweb_widget.population_in_box" in functions_sql
    assert "inciweb_widget.aqi_cats_in_box" in functions_sql
    assert "inciweb_widget.refresh_device_last_80_hourly_measurements" in functions_sql
    assert (
        "CREATE MATERIALIZED VIEW inciweb_widget.devices_last_80_hourly_measurements"
        in views_sql
    )


def test_refresh_pm25_skips_when_skip_mv_refresh_set(monkeypatch):
    monkeypatch.setenv("SKIP_MV_REFRESH", "true")
    with patch("inciweb_pipeline.__main__.get_airfire_db_conn") as mock_get_conn:
        refresh_pm25()
    mock_get_conn.assert_not_called()


def test_generate_payloads_returns_db_conn_to_pool_before_near_me_and_s3():
    """Verifies DB connection is returned to the pool before HTTP or S3 I/O runs."""
    events = []
    mock_pool = MagicMock()
    mock_conn = MagicMock()

    def getconn():
        events.append("getconn")
        return mock_conn

    def putconn(conn):
        assert conn is mock_conn
        events.append("putconn")

    mock_pool.getconn.side_effect = getconn
    mock_pool.putconn.side_effect = putconn

    row = [
        "inc_lock_test",
        -120.0,
        45.0,
        100,
        True,
        "WF",
        [10],
        "2026-08-22T00:00:00",
        True,
        False,
    ]

    with (
        patch("inciweb_pipeline.__main__.get_airfire_db_pool", return_value=mock_pool),
        patch.object(
            PayloadGenerator,
            "_get_last_72_pm25_cat",
            side_effect=lambda: events.append("db_last_72"),
        ),
        patch.object(
            PayloadGenerator,
            "_get_population_data",
            side_effect=lambda: events.append("db_population"),
        ),
        patch.object(
            PayloadGenerator,
            "_get_near_me_data",
            side_effect=lambda: events.append("http_near_me"),
        ),
        patch.object(
            PayloadGenerator,
            "_set_meta",
            side_effect=lambda: events.append("set_meta"),
        ),
        patch.object(
            PayloadGenerator,
            "_write_to_s3",
            side_effect=lambda: events.append("s3_write"),
        ),
    ):
        results = generate_payloads([row], max_workers=1)

    assert results == [{"id": "inc_lock_test", "status": "success"}]
    assert mock_conn.autocommit is True
    assert events == [
        "getconn",
        "db_last_72",
        "db_population",
        "putconn",
        "http_near_me",
        "set_meta",
        "s3_write",
    ]


def test_inciweb_s3_target_supports_prefix_and_bucket_slash_prefix(monkeypatch):
    from inciweb_pipeline.s3 import inciweb_s3_target

    monkeypatch.setenv("INCIWEB_BUCKET", "airfire-inciweb")
    monkeypatch.delenv("INCIWEB_S3_PREFIX", raising=False)
    assert inciweb_s3_target("inc_123") == (
        "airfire-inciweb",
        "inc_123/latest/payload.json",
    )

    monkeypatch.setenv("INCIWEB_S3_PREFIX", "shadow-dev")
    assert inciweb_s3_target("inc_123") == (
        "airfire-inciweb",
        "shadow-dev/inc_123/latest/payload.json",
    )

    monkeypatch.setenv("INCIWEB_BUCKET", "airfire-inciweb/shadow-dev")
    monkeypatch.delenv("INCIWEB_S3_PREFIX", raising=False)
    assert inciweb_s3_target("inc_123") == (
        "airfire-inciweb",
        "shadow-dev/inc_123/latest/payload.json",
    )


def test_fetch_near_me_raises_on_http_or_timeout_error():
    import pytest
    import requests
    from inciweb_pipeline.near_me_util import fetch_near_me

    with patch(
        "inciweb_pipeline.near_me_util._SESSION.get",
        side_effect=requests.exceptions.ConnectTimeout("Connection timed out"),
    ):
        with pytest.raises(requests.exceptions.ConnectTimeout):
            fetch_near_me(45.0, -120.0, 10)


def test_generate_payloads_does_not_retry_on_internal_type_error():
    mock_pg = MagicMock()
    mock_pg.generate_and_write_to_s3.side_effect = TypeError("bad operand type")

    with (
        patch(
            "inciweb_pipeline.__main__.get_airfire_db_pool", return_value=MagicMock()
        ),
        patch("inciweb_pipeline.__main__.PayloadGenerator", return_value=mock_pg),
    ):
        results = generate_payloads([["inc_type_err"]], max_workers=1)

    assert mock_pg.generate_and_write_to_s3.call_count == 1
    assert results == [
        {
            "id": "inc_type_err",
            "status": "failed to generate data -- could be due to no AQ observations",
        }
    ]
