import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from google.cloud import bigquery

from config import API_BASE_URL, TABLE

client = bigquery.Client()
limit = 100
sql_query = f"""
    SELECT
      Query, count(*) as occurences
    FROM
      {TABLE}
    GROUP BY Query
    ORDER BY occurences DESC
    LIMIT 1000;
"""
results = list(client.query(sql_query).result())
queries = [row["Query"] for row in results]
response_times = []


def send_request(query: str):
    start_time = time.time()
    result = requests.get(f"{API_BASE_URL}/related?query={query}")
    elapsed_time = time.time() - start_time
    if result.status_code != 200:
        print(f"{result.status_code}: {result.text}")
    response_times.append(elapsed_time)


# This allows us to simulate concurrent users
with ThreadPoolExecutor(max_workers=limit) as executor:
    futures = [executor.submit(send_request, query) for query in queries]
    for i, future in enumerate(as_completed(futures)):
        future.result()
        print(f"Sent {i + 1} requests")


print(f"Average response time: {sum(response_times) / len(response_times)}")
