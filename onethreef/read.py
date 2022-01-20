import os
import re
import pandas as pd
import xmltodict
import xml.etree.ElementTree as ET
from datetime import datetime as dt
from tqdm import tqdm
from io import StringIO
from onethreef.constants import storage_path, empty_df


def existing_ncs(year, quarter, absolute=True):
    try:
        if absolute:
            ncs = [os.path.join(storage_path,str(year),f'QTR{quarter}',x) for x in os.listdir(os.path.join(storage_path,str(year),f'QTR{quarter}')) if x.endswith('.nc')]
        else:
            ncs = [x for x in os.listdir(os.path.join(storage_path,str(year),f'QTR{quarter}')) if x.endswith('.nc')]
    except Exception as e:
        ncs = []
    
    return ncs

def to_xml(xml):
    it = ET.iterparse(StringIO(xml))
    for _, el in it:
        prefix, has_namespace, postfix = el.tag.partition('}')
        if has_namespace:
            el.tag = postfix
    return it

def read_nc(filename):
    ns = {
        'http://www.sec.gov/edgar/document/thirteenf/informationtable': None,
        'http://www.sec.gov/edgar/thirteenffiler': None,
        'http://www.sec.gov/edgar/common': None
    }
    
    xml_regex = re.compile(r'<XML>(.*?)<\/XML>', re.DOTALL)
    f = open(filename, 'r').read()
    r = re.findall(xml_regex, f)

    if len(r) != 2:
        r += ['<informationTable><infoTable></infoTable></informationTable>']
        
    submission, infotable = r
    
    return (
        xmltodict.parse(ET.tostring(ET.fromstring(submission.lstrip('\n')), encoding='utf-8', method='xml'), process_namespaces=True, namespaces=ns),
        xmltodict.parse(ET.tostring(ET.fromstring(infotable.lstrip('\n')), encoding='utf-8', method='xml'), process_namespaces=True, namespaces=ns)
    )
    
def process_infotable(infotable, filing_id=None):
    if type(infotable['informationTable']['infoTable']) != list:
        df_raw = pd.DataFrame([infotable['informationTable']['infoTable']])
    else:
        df_raw = pd.DataFrame(infotable['informationTable']['infoTable'])

    if df_raw.shape == (1,1):
        return empty_df

    for col in ['putCall', 'otherManager']:
        if col not in df_raw.columns:
            df_raw[col] = None
    
    try:
        df = (
            pd
            .concat(
                [
                    df_raw.drop(columns=['shrsOrPrnAmt','votingAuthority']),
                    df_raw['shrsOrPrnAmt'].apply(pd.Series),
                    df_raw['votingAuthority'].apply(pd.Series),
                ]
            , axis=1 )
            .reset_index()
            .rename(columns=str.lower)
            .rename(columns={'none':'nonne','index':'portfolio_id'})
            .assign(
                value = lambda df: df.value.astype(float).astype(int),
                sshprnamt = lambda df: df.sshprnamt.astype(float).astype(int),
                sole = lambda df: df.sole.astype(float).astype(int),
                shared = lambda df: df.shared.astype(float).astype(int),
                nonne = lambda df: df.nonne.astype(float).astype(int),
                nameofissuer = lambda df: df.nameofissuer.str.strip().str.upper(),
                titleofclass = lambda df: df.titleofclass.str.strip().str.upper(),
                cusip = lambda df: df.cusip.str.strip().str.upper(),
                putcall = lambda df: df.putcall.str.strip().str.upper(),
                investmentdiscretion = lambda df: df.investmentdiscretion.str.strip().str.upper(),
                sshprnamttype = lambda df: df.sshprnamttype.str.strip().str.upper()
            )
            [['portfolio_id','nameofissuer','titleofclass','cusip','value','sshprnamt','sshprnamttype','investmentdiscretion','sole','shared','nonne','putcall','othermanager']]
        )
    except Exception as e:
        print(str(e))

    if filing_id is None:
        return df
    else:
        return df.assign(filing_id = filing_id)

def process_submission(submission):
    return {
        'cik':submission['edgarSubmission']['headerData']['filerInfo']['filer']['credentials']['cik'],
        'name':submission['edgarSubmission']['formData']['coverPage']['filingManager']['name'],
        'street1':submission['edgarSubmission']['formData']['coverPage']['filingManager']['address'].get('street1'),
        'street2':submission['edgarSubmission']['formData']['coverPage']['filingManager']['address'].get('street2'),
        'city':submission['edgarSubmission']['formData']['coverPage']['filingManager']['address'].get('city'),
        'stateOrCountry':submission['edgarSubmission']['formData']['coverPage']['filingManager']['address'].get('stateOrCountry'),
        'zipCode':submission['edgarSubmission']['formData']['coverPage']['filingManager']['address'].get('zipCode'),
        'fileNumber':submission['edgarSubmission']['formData']['coverPage']['form13FFileNumber'],
        'periodOfReport':dt.strptime(submission['edgarSubmission']['headerData']['filerInfo'].get('periodOfReport', dt(1900,1,1)), '%m-%d-%Y'),
        'signatureDate':dt.strptime(submission['edgarSubmission']['formData']['signatureBlock'].get('signatureDate', dt(1900,1,1)),'%m-%d-%Y')
    }