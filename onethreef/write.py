from datetime import datetime as dt
from typing import Optional

import psycopg2
import psycopg2.extras
from sqlmodel import Field, SQLModel, select


class Company(SQLModel, table=True):
    """Class for the PostgreSQL's 'company' relation.

    Includes data of all companies that have a recorded filing in
    any of the fetched years. The relation contains an unique company_id,
    which serves as the primary key, a unique CIK, which is the SEC's way
    of referencing institutions, and additional data on the company's location.

    """

    company_id: Optional[int] = Field(default=None, primary_key=True)
    cik: str
    name: str
    street1: str
    street2: str
    city: str
    stateorcountry: str
    zipcode: int


class Filing(SQLModel, table=True):
    """Class for the PostgreSQL's 'filing' relation.

    Includes data of each company's individual filings. The relation contains
    an unique filing_id, which serves as the primary key, a company_id, which
    serves as the foreign key to the Company relation, and additional data on
    the filing's identifiers and relevant dates.

    """

    filing_id: Optional[int] = Field(default=None, primary_key=True)
    company_id: int
    filenumber: str
    accnumber: str
    periodofreport: dt
    signaturedate: dt


def run_query(conn, query):
    """Helper function to run a fetch all query using a psycopg2 connection.

    Args:
        conn (psycopg2.extensions.connection): The psycopg2 connection.
        query (str): The query string.

    Returns:
        list: The query result.

    """

    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def check_company_exists(session, cik):
    """Database query to check if a company already has a record.

    Args:
        session (sqlmodel.orm.session.Session): A SQLModel session.
        cik (str): A unique company identifier (e.g. 0001162781).

    Returns:
        None if doesn't exist, company_id if it does

    """

    q = select(Company.company_id).where(Company.cik == cik)
    return session.exec(q).first()


def add_company(session, submission_dict):
    """Adds a company record the the Company relation.

    Args:
        session (sqlmodel.orm.session.Session): A SQLModel session.
        submission_dict (dict): A dictionary containing all required submission data.

    Returns:
        Nothing.

    """

    company = Company(
        cik=submission_dict["cik"],
        name=submission_dict["name"],
        street1=submission_dict["street1"],
        street2=submission_dict["street2"],
        city=submission_dict["city"],
        stateorcountry=submission_dict["stateOrCountry"],
        zipcode=submission_dict["zipCode"],
    )
    session.add(company)


def check_filing_exists(session, accnumber):
    """Database query to check if a filing already has a record.

    Args:
        session (sqlmodel.orm.session.Session): A SQLModel session.
        accnumber (str): A unique accnumber (e.g. 0001162781-22-000001).

    Returns:
        None if doesn't exist, filing_id if it does

    """

    q = select(Filing.filing_id).where(Filing.accnumber == accnumber)
    return session.exec(q).first()


def add_filing(session, submission_dict, company_id, accnumber):
    """Adds a filing record the the filing relation.

    Args:
        session (sqlmodel.orm.session.Session): A SQLModel session.
        submission_dict (dict): A dictionary containing all required submission data.

    Returns:
        Nothing.

    """

    filing = Filing(
        company_id=company_id,
        filenumber=submission_dict["fileNumber"],
        accnumber=accnumber,
        periodofreport=submission_dict["periodOfReport"],
        signaturedate=submission_dict["signatureDate"],
    )
    session.add(filing)


def check_portfolio_exists(conn, cik):
    """Database query to check if a company's portfolio already exists.

    Args:
        conn (psycopg2.extensions.connection): The psycopg2 connection.
        cik (str): A unique company identifier (e.g. 0001162781).

    Returns:
        False if doesn't exist, True if it does

    """

    q = """
    SELECT EXISTS(
        SELECT *
        FROM information_schema.tables
        WHERE
        table_name = 'c{}'
    );
    """

    return run_query(conn, q.format(cik))[0][0]


def create_portfolio_table(conn, cik):
    """Database query to create a company's portfolio based on its cik.

    Args:
        conn (psycopg2.extensions.connection): The psycopg2 connection.
        cik (str): A unique company identifier (e.g. 0001162781).

    Returns:
        Nothing.

    """

    q = """
    CREATE TABLE c{} (
        portfolio_id INTEGER,
        nameofissuer VARCHAR,
        titleofclass VARCHAR,
        cusip VARCHAR,
        value BIGINT,
        sshprnamt BIGINT,
        sshprnamttype VARCHAR,
        investmentdiscretion VARCHAR,
        sole BIGINT,
        shared BIGINT,
        nonne BIGINT,
        putcall VARCHAR,
        othermanager VARCHAR,
        filing_id BIGINT,
        PRIMARY KEY (portfolio_id,filing_id),
        FOREIGN KEY (filing_id) REFERENCES filing(filing_id)
    );
    """
    with conn.cursor() as cur:
        cur.execute(q.format(cik))
    conn.commit()


def add_portfolio(conn, df, table):
    """Database query that uses psycopg2.extras.execute_values() to insert a dataframe.

    Args:
        conn (psycopg2.extensions.connection): The psycopg2 connection.
        df (pd.DataFrame): The dataframe to insert.
        table(str): The table name (e.g. c0001162781).

    Returns:
        Nothing.

    """

    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ",".join(list(df.columns))
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()

    try:
        psycopg2.extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
    cursor.close()
