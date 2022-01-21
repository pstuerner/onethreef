import asyncio
import os

import typer
from sqlmodel import Session
from tqdm import tqdm

from onethreef import config, fetch
from onethreef.constants import _create_engine, _init_connection, storage_path
from onethreef.read import existing_ncs, process_infotable, process_submission, read_nc
from onethreef.write import (
    add_company,
    add_filing,
    add_portfolio,
    check_company_exists,
    check_filing_exists,
    check_portfolio_exists,
    create_portfolio_table,
)

app = typer.Typer()


@app.command()
def download(year, quarter, date=None):
    """CLI entrypoint for the 'download' command.
    E.g. following command
    $ onethreef download 2016 1
    downloads all filings from the first quarter of 2016.
    The date argument can be used to only download a single feed.

    Args:
        year (int): The year.
        quarter (int): The quarter.
        date (str): The date of a single feed (e.g. 20150102).

    Returns:
        Nothing.

    """

    if date is None:
        typer.echo(f"\n\n############# {year}/QTR{quarter} #############")
        dates, accnos = fetch.fetch_index(year, quarter)
    else:
        typer.echo(f"\n\n############# {year}/QTR{quarter}/{date} #############")
        dates = [date]

    asyncio.run(fetch.download_feeds(year, quarter, dates, MAX_TASKS=10))


@app.command()
def unpack(year, quarter, date=None, delete_feeds=False):
    """CLI entrypoint for the 'unpack' command.
    E.g. the following command
    $ onethreef unpack 2015 1 --date 20150102 --delete_feeds true
    Unpacks the 20150102.nc.tar.gz in the 2015/QTR1 directory and deletes the
    feed file afterwards.

    Args:
        year (int): The year.
        quarter (int): The quarter.
        date (str): The date of a single feed (e.g. 20150102).
        delete_feeds (bool): Deletes the feeds after extraction if True.

    Returns:
        Nothing.

    """

    dates, accnos = fetch.fetch_index(year, quarter)
    if date is None:
        typer.echo(f"\n\n############# {year}/{quarter} #############")
        fetch.extract_quarter(
            year, quarter, files_to_extract=accnos, delete_feeds=delete_feeds
        )
    else:
        typer.echo(f"\n\n############# {year}/{quarter}/{date} #############")
        fetch.extract_feed(
            os.path.join(
                config.storage_path, str(year), f"QTR{quarter}", f"{date}.nc.tar.gz"
            )
        )


@app.command()
def to_database(year, quarter, filename=None):
    """CLI entrypoint for the 'to-database' command.
    E.g. the following command
    $ onethreef 2016 1
    writes all .nc files in 2016/QTR1 to the database.

    Args:
        year (int): The year.
        quarter (int): The quarter.
        filename (str): Filename of a .nc file to only write this specific file to the database.

    Returns:
        Nothing.

    """

    engine = _create_engine()

    if filename is None:
        ncs = existing_ncs(year, quarter)
    else:
        ncs = [os.path.join(storage_path, str(year), f"QTR{quarter}", filename)]

    conn = _init_connection()
    with Session(engine) as sess:
        for nc in tqdm(ncs):
            s, i = read_nc(nc)
            s_dict = process_submission(s)

            # Company
            company_id = check_company_exists(sess, s_dict["cik"])
            if company_id:
                pass
            else:
                add_company(sess, s_dict)
                company_id = check_company_exists(sess, s_dict["cik"])

            # Filing
            filing_id = check_filing_exists(sess, nc.split("/")[-1].rstrip(".nc"))
            if filing_id:
                pass
            else:
                add_filing(
                    sess,
                    s_dict,
                    company_id=company_id,
                    accnumber=nc.split("/")[-1].rstrip(".nc"),
                )
                filing_id = check_filing_exists(sess, nc.split("/")[-1].rstrip(".nc"))

            sess.commit()

            # Portfolio
            if check_portfolio_exists(conn, s_dict["cik"]):
                pass
            else:
                print(nc)
                create_portfolio_table(conn, s_dict["cik"])
                df = process_infotable(i, filing_id=filing_id)
                add_portfolio(conn, df, f"c{s_dict['cik']}")

    conn.commit()
    conn.close()


def main():
    """The main CLI entrypoint"""

    app()


if __name__ == "__main__":
    main()
