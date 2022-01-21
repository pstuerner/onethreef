import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime as dt

import pandas as pd
import xmltodict

from onethreef.constants import empty_df, storage_path


def existing_ncs(year, quarter, absolute=True):
    """A function that returns all .nc files of a given year & quarter.

    Args:
        year (int): The year.
        quarter (int): The quarter.
        absolute (bool): Whether or not to return the file's absolute path.

    Returns:
        list: A list of filenames or absolute paths

    """

    try:
        if absolute:
            ncs = [
                os.path.join(storage_path, str(year), f"QTR{quarter}", x)
                for x in os.listdir(
                    os.path.join(storage_path, str(year), f"QTR{quarter}")
                )
                if x.endswith(".nc")
            ]
        else:
            ncs = [
                x
                for x in os.listdir(
                    os.path.join(storage_path, str(year), f"QTR{quarter}")
                )
                if x.endswith(".nc")
            ]
    except Exception:
        ncs = []

    return ncs


def read_nc(filename):
    """A function that reads a .nc file and splits it into a submission and info table.
    The submission table includes information on the filing's identifiers and relevant dates.
    The info table includes the filing's portfolio.

    Args:
        filename (str): The absolute filepath.

    Returns:
        tuple(dict, dict): The first dictionary contains the submission and the second the info table.

    """

    ns = {
        "http://www.sec.gov/edgar/document/thirteenf/informationtable": None,
        "http://www.sec.gov/edgar/thirteenffiler": None,
        "http://www.sec.gov/edgar/common": None,
    }

    xml_regex = re.compile(r"<XML>(.*?)<\/XML>", re.DOTALL)
    f = open(filename, "r").read()
    r = re.findall(xml_regex, f)

    if len(r) != 2:
        r += ["<informationTable><infoTable></infoTable></informationTable>"]

    submission, infotable = r

    return (
        xmltodict.parse(
            ET.tostring(
                ET.fromstring(submission.lstrip("\n")), encoding="utf-8", method="xml"
            ),
            process_namespaces=True,
            namespaces=ns,
        ),
        xmltodict.parse(
            ET.tostring(
                ET.fromstring(infotable.lstrip("\n")), encoding="utf-8", method="xml"
            ),
            process_namespaces=True,
            namespaces=ns,
        ),
    )


def process_infotable(infotable, filing_id=None):
    """A function that processes the raw info table dictionary.
    The function first checks for edge scenarios such as an empty portfolio or
    with only one entry. Second, sometimes columns are missing in the info table
    as they're not part of the filing. The function appends missing columns to keep
    the database entries consistent. Third, preprocessing. Explode nested columns,
    rename columns, add portfolio_id, convert str to int (first to float due to
    formatting issues), convert str to upper str, correctly order the columns.

    Args:
        infotable (dict): A dictionary with the filing's info table.
        filing_id (str): The portfolio's filing_id. Appends an extra column to the
            dataframe containing the filing_id which is part of the relation's primary key.
            Only required for database purposes.

    Returns:
        pd.DataFrame: The preprocessed dataframe.


    """

    if type(infotable["informationTable"]["infoTable"]) != list:
        df_raw = pd.DataFrame([infotable["informationTable"]["infoTable"]])
    else:
        df_raw = pd.DataFrame(infotable["informationTable"]["infoTable"])

    if df_raw.shape == (1, 1):
        return empty_df

    for col in ["putCall", "otherManager"]:
        if col not in df_raw.columns:
            df_raw[col] = None

    try:
        df = (
            pd.concat(
                [
                    df_raw.drop(columns=["shrsOrPrnAmt", "votingAuthority"]),
                    df_raw["shrsOrPrnAmt"].apply(pd.Series),
                    df_raw["votingAuthority"].apply(pd.Series),
                ],
                axis=1,
            )
            .reset_index()
            .rename(columns=str.lower)
            .rename(columns={"none": "nonne", "index": "portfolio_id"})
            .assign(
                value=lambda df: df.value.astype(float).astype(int),
                sshprnamt=lambda df: df.sshprnamt.astype(float).astype(int),
                sole=lambda df: df.sole.astype(float).astype(int),
                shared=lambda df: df.shared.astype(float).astype(int),
                nonne=lambda df: df.nonne.astype(float).astype(int),
                nameofissuer=lambda df: df.nameofissuer.str.strip().str.upper(),
                titleofclass=lambda df: df.titleofclass.str.strip().str.upper(),
                cusip=lambda df: df.cusip.str.strip().str.upper(),
                putcall=lambda df: df.putcall.str.strip().str.upper(),
                investmentdiscretion=lambda df: df.investmentdiscretion.str.strip().str.upper(),
                sshprnamttype=lambda df: df.sshprnamttype.str.strip().str.upper(),
            )[
                [
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
                ]
            ]
        )
    except Exception as e:
        print(str(e))

    if filing_id is None:
        return df
    else:
        return df.assign(filing_id=filing_id)


def process_submission(submission):
    """A functio to extract relevant information from the filing's submission table.

    Args:
        submission (dict): The submission dictionary.

    Returns:
        dict: A reduced dictionary with only relevant information.


    """

    return {
        "cik": submission["edgarSubmission"]["headerData"]["filerInfo"]["filer"][
            "credentials"
        ]["cik"],
        "name": submission["edgarSubmission"]["formData"]["coverPage"]["filingManager"][
            "name"
        ],
        "street1": submission["edgarSubmission"]["formData"]["coverPage"][
            "filingManager"
        ]["address"].get("street1"),
        "street2": submission["edgarSubmission"]["formData"]["coverPage"][
            "filingManager"
        ]["address"].get("street2"),
        "city": submission["edgarSubmission"]["formData"]["coverPage"]["filingManager"][
            "address"
        ].get("city"),
        "stateOrCountry": submission["edgarSubmission"]["formData"]["coverPage"][
            "filingManager"
        ]["address"].get("stateOrCountry"),
        "zipCode": submission["edgarSubmission"]["formData"]["coverPage"][
            "filingManager"
        ]["address"].get("zipCode"),
        "fileNumber": submission["edgarSubmission"]["formData"]["coverPage"][
            "form13FFileNumber"
        ],
        "periodOfReport": dt.strptime(
            submission["edgarSubmission"]["headerData"]["filerInfo"].get(
                "periodOfReport", dt(1900, 1, 1)
            ),
            "%m-%d-%Y",
        ),
        "signatureDate": dt.strptime(
            submission["edgarSubmission"]["formData"]["signatureBlock"].get(
                "signatureDate", dt(1900, 1, 1)
            ),
            "%m-%d-%Y",
        ),
    }
