"""PostgreSQL connection/engine helpers for the Inciweb pipeline."""

import os
from urllib.parse import quote_plus

import psycopg2
from dotenv import load_dotenv

load_dotenv()


REQUIRED_KEYS = {"host", "user", "password", "database"}


def _get_config(env_vars):
    if not isinstance(env_vars, dict):
        raise ValueError(
            f"env_vars must be a dict mapping keys to env var names. "
            f"Required keys: {REQUIRED_KEYS}. Optional: port"
        )

    missing_keys = REQUIRED_KEYS - env_vars.keys()
    if missing_keys:
        raise ValueError(f"env_vars missing required keys: {missing_keys}.")

    missing_env = [
        v for k, v in env_vars.items() if k in REQUIRED_KEYS and not os.getenv(v)
    ]
    if missing_env:
        raise ValueError(f"Missing env var(s): {', '.join(missing_env)}")

    return {
        "host": os.getenv(env_vars["host"]),
        "port": os.getenv(env_vars.get("port"), "5432"),
        "user": os.getenv(env_vars["user"]),
        "password": os.getenv(env_vars["password"]),
        "database": os.getenv(env_vars["database"]),
    }


def get_uri(env_vars, sslmode="require"):
    cfg = _get_config(env_vars)
    pw = quote_plus(cfg["password"])
    uri = (
        f"postgresql://{cfg['user']}:{pw}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )
    return f"{uri}?sslmode={sslmode}" if sslmode else uri


def get_conn(env_vars, options=None, connect_timeout=20):
    cfg = _get_config(env_vars)
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
        connect_timeout=connect_timeout,
        options=options,
    )


AIRFIRE_DB = {
    "host": "AIRFIRE_DB_HOST",
    "port": "AIRFIRE_DB_PORT",
    "user": "AIRFIRE_DB_USER",
    "password": "AIRFIRE_DB_PW",
    "database": "AIRFIRE_DB_DATABASE",
}

STATEMENT_TIMEOUT = "-c statement_timeout=60000"


def airfire_pg_uri(sslmode=None):
    if sslmode is None:
        sslmode = os.getenv("PGSSLMODE", "require")
    return get_uri(AIRFIRE_DB, sslmode=sslmode)


def get_airfire_db_conn(options=None):
    return get_conn(AIRFIRE_DB, options)
