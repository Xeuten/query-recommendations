from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import bigquery, datastore

from config import TABLE

bigquery_client = bigquery.Client()
datastore_client = datastore.Client()


query = f"""
    WITH

    total_sessions AS (
      SELECT COUNT(DISTINCT session_id) AS total_sess
      FROM {TABLE}
    ),

    query_freq AS (
      SELECT 
        query,
        COUNT(DISTINCT session_id) AS sess_count
      FROM {TABLE}
      GROUP BY query
    ),

    session_queries AS (
      SELECT 
        session_id, 
        ARRAY_AGG(DISTINCT query) AS queries
      FROM {TABLE}
      GROUP BY session_id
    ),

    pair_freq AS (
      SELECT
        q1 AS query_1,
        q2 AS query_2,
        COUNT(*) AS pair_sess_count
      FROM session_queries,
           UNNEST(queries) AS q1,
           UNNEST(queries) AS q2
      WHERE q1 != q2
      GROUP BY q1, q2
    ),

    pmi_results AS (
      SELECT 
      pair_freq.query_1, 
      pair_freq.query_2,
      SAFE.LOG(
        (pair_sess_count / total_sess) 
        / ((query_freq_q1.sess_count / total_sess) * (query_freq_q2.sess_count / total_sess))
      ) AS PMI,
      query_freq_q2.sess_count AS q2_sess_count
    FROM pair_freq
    JOIN query_freq AS query_freq_q1 ON pair_freq.query_1 = query_freq_q1.query
    JOIN query_freq AS query_freq_q2 ON pair_freq.query_2 = query_freq_q2.query
    CROSS JOIN total_sessions
    ORDER BY PMI DESC
    ),

    top_k_related AS (
        SELECT
            query_1,
            ARRAY_AGG(query_2 ORDER BY PMI DESC, q2_sess_count DESC LIMIT 3) AS related_queries
        FROM pmi_results
        WHERE q2_sess_count > 2
        GROUP BY query_1
    )

    SELECT * FROM top_k_related;
"""


rows = list(bigquery_client.query(query).result())
rows_len = len(rows)


def process_row(row):
    try:
        query_key = row["query_1"]
        related_queries = row["related_queries"]
        entity = datastore.Entity(datastore_client.key("TestQuery1", query_key))
        entity["related_queries"] = related_queries
        datastore_client.put(entity)
        return True
    except Exception as e:
        print(f"Error processing row {row}: {e}")
        return False


with ThreadPoolExecutor(max_workers=12) as executor:
    futures = [executor.submit(process_row, row) for row in rows]
    for i, future in enumerate(as_completed(futures)):
        try:
            result = future.result()
            if result:
                print(f"Processed {i + 1} / {rows_len} rows ({(i + 1) / rows_len:.2%})")
            else:
                print(f"Failed to process row {i + 1}")
        except Exception as e:
            print(f"Exception during row processing: {e}")
