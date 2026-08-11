from unittest.mock import MagicMock, patch

from inciweb_pipeline.__main__ import generate_payloads, get_incident_rows, refresh_pm25
from inciweb_pipeline.constants import WIDGET_SCHEMA


def test_refresh_pm25():
    mock_conn = MagicMock()
    mock_curr = MagicMock()
    mock_conn.cursor.return_value = mock_curr

    with patch("inciweb_pipeline.__main__.get_airfire_db_conn", return_value=mock_conn):
        refresh_pm25()

    mock_curr.execute.assert_called_once_with(
        f"SELECT {WIDGET_SCHEMA}.refresh_device_last_80_hourly_measurements();"
    )
    mock_conn.commit.assert_called_once()
    mock_curr.close.assert_called_once()


def test_get_incident_rows():
    mock_im = MagicMock()
    mock_im.to_rows.return_value = [["inc_1", -120.0, 45.0]]

    with patch("inciweb_pipeline.__main__.IncidentManager", return_value=mock_im):
        rows = get_incident_rows()

    assert rows == [["inc_1", -120.0, 45.0]]
    mock_im.get_incidents.assert_called_once()


def test_generate_payloads_success_and_failure():
    rows = [["inc_101"], ["inc_102"]]

    def mock_pg_init(row):
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
