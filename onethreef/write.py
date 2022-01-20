from typing import Optional
from datetime import datetime as dt
import psycopg2
import psycopg2.extras
from sqlmodel import Field, SQLModel, select


class Company(SQLModel, table=True):
    company_id: Optional[int] = Field(default=None, primary_key=True)
    cik: str
    name: str
    street1: str
    street2: str
    city: str
    stateorcountry: str
    zipcode: int

class Filing(SQLModel, table=True):
    filing_id: Optional[int] = Field(default=None, primary_key=True)
    company_id: int
    filenumber: str
    accnumber: str
    periodofreport: dt
    signaturedate: dt

def run_query(conn, query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

def check_company_exists(session, cik):
    q = select(Company.company_id).where(Company.cik==cik)
    r = session.exec(q).first()

    if r is None:
        return False
    else:
        return r

def add_company(session, submission_dict):
    company = Company(
        cik = submission_dict['cik'],
        name = submission_dict['name'],
        street1 = submission_dict['street1'],
        street2 = submission_dict['street2'],
        city = submission_dict['city'],
        stateorcountry = submission_dict['stateOrCountry'],
        zipcode = submission_dict['zipCode']
    )
    session.add(company)

def check_filing_exists(session, accnumber):
    q = select(Filing.filing_id).where(Filing.accnumber==accnumber)
    return session.exec(q).first()

def add_filing(session, submission_dict, company_id, accnumber):
    filing = Filing(
        company_id = company_id,
        filenumber = submission_dict['fileNumber'],
        accnumber = accnumber,
        periodofreport = submission_dict['periodOfReport'],
        signaturedate = submission_dict['signatureDate']
    )
    session.add(filing)

def check_portfolio_exists(conn, cik):
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
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        psycopg2.extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()