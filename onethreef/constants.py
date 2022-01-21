from pathlib import Path

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from onethreef import config

headers = {
    "User-Agent": f"{config.name} {config.email}",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov",
}
index_url = "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/form.idx"
storage_path = Path(config.storage_path)
ns = {
    "": "http://www.sec.gov/edgar/thirteenffiler",
    "com": "http://www.sec.gov/edgar/common",
}
empty_df = pd.DataFrame(
    columns=[
        "portfolio_id",
        "nameofissuer",
        "titleofclass",
        "cusip",
        "value",
        "sshprnamt",
        "sshprnamttype",
        "investmentdiscretion",
        "sole",
        "shared",
        "nonne",
        "putcall",
        "othermanager",
        "filing_id",
    ]
)


def _create_engine():
    """A helper function that creates a SQLAlchemy engine. Used for SQLModels."""

    return create_engine(
        f"postgresql://{config.postgres_user}:{config.postgres_pwd}@{config.postgres_ip}:{config.postgres_port}/{config.postgres_db}"
    )


def _init_connection():
    """A helper function to make a psycopg2 connection to the database."""
    return psycopg2.connect(
        f"postgresql://{config.postgres_user}:{config.postgres_pwd}@{config.postgres_ip}:{config.postgres_port}/{config.postgres_db}"
    )
