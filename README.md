<center>
    <b>
        <pre>
                   _   _                    __
   ___  _ __   ___| |_| |__  _ __ ___  ___ / _|
  / _ \| '_ \ / _ \ __| '_ \| '__/ _ \/ _ \ |_
 | (_) | | | |  __/ |_| | | | | |  __/  __/  _|
  \___/|_| |_|\___|\__|_| |_|_|  \___|\___|_|
        </pre>
    </b>
<center>

---

<center>
    <b>
        A package to download, store, and analyze 13F filings
    </b>
</center>

---

## What is this?
<code>onethreef</code> is a Python package that includes a command-line interface to automate downloading and storing [https://www.investor.gov/introduction-investing/investing-basics/glossary/form-13f-reports-filed-institutional-investment](13F filings) in a PostgreSQL database.

## How to use it?
1. Clone the package with <code>$ git clone https://github.com/pstuerner/onethreef.git</code>
2. <code>cd</code> to the root folder (where the <code>setup.py</code> is located) and run <code>$ pip install -e .</code>
3. Change your configs in <code>onethreef/onethreef/config.py</code>. Name and email are required for the request headers. Otherwise the SEC will block your requests. Storage path is refers to the mounted volume of the PostgreSQL container. The remaining configs refer to your PostgreSQL instance
4. Use the command-line interface to download, unpack, and write to the database

## Example usage

Download all feeds from the first quarter of 2016
<code>$ onethreef download 2016 1</code>

Unpack all feeds from the first quarter of 2016
<code>$ onethreef unpack 2016 1</code>

Write all filings the first quarter of 2016 to the database
<code>$ onethreef to-database 2016 1</code>

## Work in progress
- Dockerfile for the PostgreSQL
- docker-compose for both, the application and the database
- analytics functionalities to work with the donwloaded data
