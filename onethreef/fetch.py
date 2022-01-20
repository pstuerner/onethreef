import re
import os
import requests
import pathlib
import tarfile
import asyncio
import aiofiles
import aiohttp
from tqdm import tqdm
from onethreef.constants import index_url, headers, storage_path


def fetch_index(year, quarter, form_type=['13F-HR','13F-HR/A']):
    req = requests.get(index_url.format(year=year, quarter=quarter), headers=headers)

    if req.status_code == 200:
        accno_regex = re.compile(r'\d+-\d+-\d+.txt')
        date_regex = re.compile(r'\d{4}-\d{2}-\d{2}')
        dates = set()
        accnos = []
        for line in req.text.split('\n'):
            if line.startswith(tuple(form_type)):
                dates.add(date_regex.search(line.strip()).group().replace('-',''))
                accnos.append(accno_regex.search(line.strip()).group()[:-4])

        return dates, accnos

def existing_feeds(year, quarter):
    try:
        return [x for x in os.listdir(os.path.join(storage_path,str(year),f'QTR{quarter}')) if x.endswith('.tar.gz')]
    except Exception as e:
        return []

async def download_feed(url, sem):
    async with sem:
        async with aiohttp.ClientSession() as session:
            print('downloading',url)
            async with session.get(url, headers=headers, timeout=None) as response:
                async for chunk in response.content.iter_chunked(4096):
                    yield chunk

async def save_feed(path, chunk_iter):
    async with aiofiles.open(path, 'wb') as f:
        async for chunk in chunk_iter:
            await f.write(chunk)
    
async def download_feeds(year, quarter, dates=[], MAX_TASKS=5):
    tasks = []
    sem = asyncio.Semaphore(MAX_TASKS)
    r = requests.get(f'https://www.sec.gov/Archives/edgar/Feed/{year}/QTR{quarter}/', headers=headers)
    feed_regex=re.compile(r'\d+.nc.tar.gz')
    feeds = {x.group() for x in re.finditer(feed_regex, r.text)}
    pathlib.Path(os.path.join(storage_path, str(year), f'QTR{quarter}')).mkdir(parents=True, exist_ok=True)
    
    if len(dates) > 0:
        feeds = sorted([feed for feed in feeds if feed.split('.')[0] in dates])

    for feed in feeds:
        url = f'https://www.sec.gov/Archives/edgar/Feed/{year}/QTR{quarter}/{feed}'
        url_split = url.split('/')
        path = os.path.join(storage_path, url_split[-3], url_split[-2], url_split[-1])
        tasks.append(save_feed(path, download_feed(url, sem)))
    await asyncio.gather(*tasks)

def extract_feed(filename, files_to_extract=[]):
    t = tarfile.open(filename, 'r:gz')
    accno_regex = re.compile(r'\d+-\d+-\d+')

    for member in tqdm(t.getmembers(), desc=filename.split('/')[-1]):
        accno = accno_regex.search(member.name)
        if accno is not None:
            if accno.group() in files_to_extract:
                files_to_extract.remove(accno.group())
                t.extract(member, os.path.join('/', *filename.split('/')[:-1]))      
        
def extract_quarter(year, quarter, files_to_extract=[], delete_feeds=False):
    feeds = existing_feeds(year, quarter)
    dates, accnos = fetch_index(year, quarter)
    for feed in feeds:
        extract_feed(os.path.join(storage_path, str(year), f'QTR{quarter}', feed), files_to_extract=files_to_extract)
        if delete_feeds:
            os.remove(os.path.join(storage_path, str(year), f'QTR{quarter}', feed))