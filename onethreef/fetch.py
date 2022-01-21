import asyncio
import os
import pathlib
import re
import tarfile

import aiofiles
import aiohttp
import requests
from tqdm import tqdm

from onethreef.constants import headers, index_url, storage_path


def fetch_index(year, quarter, form_type=["13F-HR", "13F-HR/A"]):
    """A function that fetches a form.idx file from SEC EDGAR.
    The form.idx file (e.g. https://www.sec.gov/Archives/edgar/full-index/2013/QTR1/form.idx)
    contains all filings of a quarter. This function downloads the file
    and extracts all publishing dates and acc numbers of filings that match
    the specified form_type:

    Args:
        year (int): The year.
        quarter (int): The quarter.
        form_type (list): A list of SEC form types to extract (e.g. 13F-HR, 10K)

    Returns:
        tuple(set, list): The first element is a unique list (set) of publishing dates,
            the second element is a list of all filing's acc numbers.

    """

    req = requests.get(index_url.format(year=year, quarter=quarter), headers=headers)

    if req.status_code == 200:
        accno_regex = re.compile(r"\d+-\d+-\d+.txt")
        date_regex = re.compile(r"\d{4}-\d{2}-\d{2}")
        dates = set()
        accnos = []
        for line in req.text.split("\n"):
            if line.startswith(tuple(form_type)):
                dates.add(date_regex.search(line.strip()).group().replace("-", ""))
                accnos.append(accno_regex.search(line.strip()).group()[:-4])

        return (dates, accnos)


def existing_feeds(year, quarter):
    """A helper function to list all downloaded feeds of a quarter.

    Args:
        year (int): The year.
        quarter (int): The quarter.

    Return:
        list: A list of all .tar.gz files in the quarter directory.

    """

    try:
        return [
            x
            for x in os.listdir(os.path.join(storage_path, str(year), f"QTR{quarter}"))
            if x.endswith(".tar.gz")
        ]
    except Exception:
        return []


async def download_feed(url, sem):
    """A function that downloads a feed.

    Args:
        url (str): The feed url.
        sem (asyncio.locks.Semaphore): The semaphore lock to restrict maximum number of
            downloads at the same time.

    Yields:
        bytes: Byte chunk of the file.

    """

    async with sem:
        async with aiohttp.ClientSession() as session:
            print("downloading", url)
            async with session.get(url, headers=headers, timeout=None) as response:
                async for chunk in response.content.iter_chunked(4096):
                    yield chunk


async def save_feed(path, chunk_iter):
    """A function that continuously writes chunks to disk.
    This avoids to have all feeds in memory at the same time.

    Args:
        path (str): The filepath to write the chunk to.
        chunk_iter (bytes): The bytes to write.

    Returns:
        Nothing.

    """

    async with aiofiles.open(path, "wb") as f:
        async for chunk in chunk_iter:
            await f.write(chunk)


async def download_feeds(year, quarter, dates=[], MAX_TASKS=5):
    """A function that downloads feeds from a quarter based on their publishing dates.
    The function first extracts all available feeds of a quarter. It then matches which of
    those feeds are included in the desired dates list. After this, it sets up an async
    work task based on a maximum number of tasks and downloads the feeds in parallel.

    Args:
        year (int): The year.
        quarter (int): The quarter.
        dates (list): A list of dates that'll specify which feeds to download.
        MAX_TASKS (int): The maximum number of feeds to download at the same time.

    Returns:
        Nothing. But gives you a directory with all downloaded feeds :).

    """

    tasks = []
    sem = asyncio.Semaphore(MAX_TASKS)
    r = requests.get(
        f"https://www.sec.gov/Archives/edgar/Feed/{year}/QTR{quarter}/", headers=headers
    )
    feed_regex = re.compile(r"\d+.nc.tar.gz")
    feeds = {x.group() for x in re.finditer(feed_regex, r.text)}
    pathlib.Path(os.path.join(storage_path, str(year), f"QTR{quarter}")).mkdir(
        parents=True, exist_ok=True
    )

    if len(dates) > 0:
        feeds = sorted([feed for feed in feeds if feed.split(".")[0] in dates])

    for feed in feeds:
        url = f"https://www.sec.gov/Archives/edgar/Feed/{year}/QTR{quarter}/{feed}"
        url_split = url.split("/")
        path = os.path.join(storage_path, url_split[-3], url_split[-2], url_split[-1])
        tasks.append(save_feed(path, download_feed(url, sem)))
    await asyncio.gather(*tasks)


def extract_feed(filename, files_to_extract=[]):
    """A function that extracts a list of acc numbers from a feed.
    This is used to reduce the file size on disk. Feeds are large since they contain
    all filings of the entire quarter. By only extracting the required filings the
    disk consumption can be reduced dramatically. All filings found in the .tar.gz
    file are extracted into the same directory.

    Args:
        filename (str): The absolute path of the feed.
        files_to_extract (list): A list of acc numbers.

    Returns:
        Nothing. But gives you all the filings included in the feed :).


    """

    t = tarfile.open(filename, "r:gz")
    accno_regex = re.compile(r"\d+-\d+-\d+")

    for member in tqdm(t.getmembers(), desc=filename.split("/")[-1]):
        accno = accno_regex.search(member.name)
        if accno is not None:
            if accno.group() in files_to_extract:
                files_to_extract.remove(accno.group())
                t.extract(member, os.path.join("/", *filename.split("/")[:-1]))


def extract_quarter(year, quarter, files_to_extract=[], delete_feeds=False):
    """A function that extracts all relevant filings from an entire quarter.

    Args:
        year (int): The year.
        quarter (int): The quarter.
        files_to_extract (list): A list of acc numbers.
        delete_feeds (bool): Deletes the feeds after extraction if True. Saves disk space.

    Returns:
        Nothing.

    """

    feeds = existing_feeds(year, quarter)
    dates, accnos = fetch_index(year, quarter)
    for feed in feeds:
        extract_feed(
            os.path.join(storage_path, str(year), f"QTR{quarter}", feed),
            files_to_extract=files_to_extract,
        )
        if delete_feeds:
            os.remove(os.path.join(storage_path, str(year), f"QTR{quarter}", feed))
