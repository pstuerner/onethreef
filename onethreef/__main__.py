import os
import typer
import asyncio
from tqdm import tqdm
from sqlmodel import Session
from onethreef import fetch
from onethreef import config
from onethreef.constants import storage_path, _create_engine, _init_connection
from onethreef.read import read_nc, process_submission, process_infotable, existing_ncs
from onethreef.write import (
    check_company_exists,
    add_company,
    check_filing_exists,
    add_filing,
    check_portfolio_exists,
    create_portfolio_table,
    add_portfolio
    )


app = typer.Typer()

@app.command()
def download(year, quarter, date=None):
    if date is None:
        typer.echo(f'\n\n############# {year}/QTR{quarter} #############')
        dates, accnos = fetch.fetch_index(year,quarter)
    else:
        typer.echo(f'\n\n############# {year}/QTR{quarter}/{date} #############')
        dates=[date]

    asyncio.run(fetch.download_feeds(year, quarter, dates, MAX_TASKS=10))

@app.command()
def unpack(year, quarter, date=None, delete_feeds=False):
    dates, accnos = fetch.fetch_index(year, quarter)
    if date is None:
        typer.echo(f'\n\n############# {year}/{quarter} #############')
        fetch.extract_quarter(year,quarter,files_to_extract=accnos,delete_feeds=delete_feeds)
    else:
        typer.echo(f'\n\n############# {year}/{quarter}/{date} #############')
        fetch.extract_feed(os.path.join(config.storage_path, str(year), f'QTR{quarter}', f'{date}.nc.tar.gz'))


@app.command()
def to_database(year, quarter, filename=None):
    engine = _create_engine()

    if filename is None:
        ncs = existing_ncs(year, quarter)
    else:
        ncs = [os.path.join(storage_path,str(year),f'QTR{quarter}',filename)]

    conn = _init_connection()
    with Session(engine) as sess:  
        for nc in tqdm(ncs):
            s, i = read_nc(nc)
            s_dict = process_submission(s)

            # Company
            company_id = check_company_exists(sess, s_dict['cik'])
            if company_id:
                pass
            else:
                add_company(sess, s_dict)
                company_id = check_company_exists(sess, s_dict['cik'])
            
            # Filing
            filing_id = check_filing_exists(sess, nc.split('/')[-1].rstrip('.nc'))
            if filing_id:
                pass
            else:
                add_filing(sess, s_dict, company_id=company_id, accnumber=nc.split('/')[-1].rstrip('.nc'))
                filing_id = check_filing_exists(sess, nc.split('/')[-1].rstrip('.nc'))

            sess.commit()

            # Portfolio
            if check_portfolio_exists(conn, s_dict['cik']):
                pass
            else:
                print(nc)
                create_portfolio_table(conn, s_dict['cik'])
                df = process_infotable(i, filing_id=filing_id)
                add_portfolio(conn, df, f"c{s_dict['cik']}")
            
    conn.commit()
    conn.close()


def main():
    app()

if __name__ == "__main__":
    main()