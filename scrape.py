#!/usr/bin/env python3

from bs4 import BeautifulSoup
import requests
import pandas as pd
import asyncio
import aiohttp
import datetime


base_url = "https://results.chicagomarathon.com/2023/"
url = f"{base_url}?pid=list&pidp=start&num_results=1000"

def get_results_table_records(
    base_url: str = base_url,
    params: dict = None,
    num_results: int = 1000,
    page: int = 1,
):
    """Get the results table from the Chicago Marathon website"""
    if not params:
        params = {
            "pid": "list",
            "pidp": "start",
            "num_results": num_results,
            "page": page,
        }
    r = requests.get(base_url, params=params)
    soup = BeautifulSoup(r.text, "html.parser")
    list_table = soup.find("ul", class_="list-group list-group-multicolumn")
    row_list = list_table.find_all("li", class_="list-active list-group-item row")
    row_list.extend(list_table.find_all("li", class_="list-group-item row"))

    records = {
        "name": [],
        "url": [],
    }

    for row in row_list:
        person = row.find("h4", class_="list-field type-fullname")
        records["name"].append(person.text)
        records["url"].append(person.find("a").get("href"))

    return records


async def get_results_table_records_async(
    base_url: str = base_url,
    params: dict = None,
    num_results: int = 1000,
    page: int = 1,
):
    """Get the results table from the Chicago Marathon website"""
    if not params:
        params = {
            "pid": "list",
            "pidp": "start",
            "num_results": num_results,
            "page": page,
        }

    print(f"{datetime.datetime.now()}: Starting getting page {page}")
    async with aiohttp.ClientSession() as session:
        async with session.get(base_url, params=params) as resp:
            text = await resp.text()
            soup = BeautifulSoup(text, "html.parser")
            list_table = soup.find("ul", class_="list-group list-group-multicolumn")
            row_list = list_table.find_all(
                "li", class_="list-active list-group-item row"
            )
            row_list.extend(list_table.find_all("li", class_="list-group-item row"))

            records = {
                "name": [],
                "url": [],
            }

            for row in row_list:
                person = row.find("h4", class_="list-field type-fullname")
                records["name"].append(person.text)
                records["url"].append(person.find("a").get("href"))
    return records


async def fetch_records():
    tasks = []
    max_concurrent = 24
    sem = asyncio.Semaphore(max_concurrent)
    for page in range(1, 49):
        await sem.acquire()
        task = asyncio.create_task(
            get_results_table_records_async(num_results=1000, page=page)
        )
        tasks.append(task)
        task.add_done_callback(lambda _: sem.release())
    results = await asyncio.gather(*tasks)
    return results


def main_records(output_file: str = "chicago_marathon_records.csv"):
    results = asyncio.run(fetch_records())
    records = {"name": [], "url": []}
    for result in results:
        records["name"].extend(result["name"])
        records["url"].extend(result["url"])
    records = pd.DataFrame(records)
    records.to_csv(output_file, index=False)
    return records


def get_results(record_url: str, base_url: str = base_url):
    """Get the results for a given record URL"""
    record_html = requests.get(f"{base_url}{record_url}").text
    soup_record = BeautifulSoup(record_html, "html.parser")

    # Participant table
    part_table = soup_record.find("table", class_="table table-condensed")

    part_rows = []
    for h in part_table.find_all("tr"):
        print(h)
        part_rows.append(h.text.strip().split("\n"))

    splits_table = soup_record.find(
        "table", class_="table table-condensed table-striped"
    )

    table_headers = []
    for split in splits_table.find("thead").find_all("th"):
        table_headers.append(split.text.strip())

    rows = []
    for split in splits_table.find("tbody").find_all("tr"):
        rows.append(split.text.strip().split("\n"))

    results = {"headers": table_headers, "rows": rows}
    return part_rows, results


def result_to_df(result: dict, part_rows):
    cols = [x[0] for x in part_rows]
    vals = [x[-1] for x in part_rows]

    df = pd.concat(
        [
            pd.DataFrame([vals], columns=cols),
            pd.DataFrame(result["rows"], columns=result["headers"]),
        ],
        axis=0,
    ).reset_index(drop=True)

    # Fill all the NaNs with the previous value
    df = df.ffill()
    return df


async def get_results_async(
    record_url: str, base_url: str = base_url, job_number: int = 0
):
    """Get the results for a given record URL"""
    print(f"{datetime.datetime.now()}: Starting job {job_number}")
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get(f"{base_url}{record_url}") as resp:
            record_html = await resp.text()
            soup_record = BeautifulSoup(record_html, "html.parser")

            # Participant table
            part_table = soup_record.find("table", class_="table table-condensed")

            part_rows = []
            for h in part_table.find_all("tr"):
                part_rows.append(h.text.strip().split("\n"))

            splits_table = soup_record.find(
                "table", class_="table table-condensed table-striped"
            )

            table_headers = []
            for split in splits_table.find("thead").find_all("th"):
                table_headers.append(split.text.strip())

            rows = []
            for split in splits_table.find("tbody").find_all("tr"):
                rows.append(split.text.strip().split("\n"))

            results = {"headers": table_headers, "rows": rows}
            print(f"{datetime.datetime.now()}: Finished job {job_number}")
            results_df = result_to_df(results, part_rows)
            results_df.to_parquet(f"data/results_{job_number}.parquet", index=False)
            return True


async def main_results(
    records_file: str = "chicago_marathon_records.csv",
    max_concurrent: int = 100,
):
    """Get the results for all the records in the records file"""
    results = pd.read_csv(records_file)
    tasks = []
    sem = asyncio.Semaphore(max_concurrent)
    for job, record in enumerate(results["url"], start=0):
        await sem.acquire()
        task = asyncio.create_task(get_results_async(record, job_number=job))
        tasks.append(task)
        task.add_done_callback(lambda _: sem.release())
    await asyncio.gather(*tasks)
    return


if __name__ == "__main__":
    asyncio.run(main_records())
    asyncio.run(main_results())
