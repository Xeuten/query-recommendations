from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import bigquery, datastore

from config import TABLE

bigquery_client = bigquery.Client()
datastore_client = datastore.Client()


query = f"""
    WITH

    -- Count the total number of sessions
    total_sessions AS (
      SELECT COUNT(DISTINCT session_id) AS total_sess
      FROM {TABLE}
    ),

    -- Count the frequency of each query
    query_freq AS (
      SELECT 
        query,
        COUNT(DISTINCT session_id) AS sess_count
      FROM {TABLE}
      GROUP BY query
    ),

    -- Collect all queries for each session into arrays
    session_queries AS (
      SELECT 
        session_id, 
        ARRAY_AGG(DISTINCT query) AS queries
      FROM {TABLE}
      GROUP BY session_id
    ),

    -- Generate pairs and count how many sessions contain each pair
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

    -- Join to get P(q1), P(q2), P(q1,q2) and compute PMI
    pmi_results AS (
      SELECT 
      pair_freq.query_1, 
      pair_freq.query_2,
      -- Compute probabilities:
      -- P(q1) = query_freq_q1.sess_count / total_sess
      -- P(q2) = query_freq_q2.sess_count / total_sess
      -- P(q1,q2) = pair_sess_count / total_sess
      -- PMI = LOG((P(q1,q2)) / (P(q1)*P(q2)))
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

    -- Collect the top 3 related queries for each query
    top_k_related AS (
        SELECT
            query_1,
            -- Use an additional ORDER BY clause to break ties and get better results
            ARRAY_AGG(query_2 ORDER BY PMI DESC, q2_sess_count DESC LIMIT 3) AS related_queries
        FROM pmi_results
        -- Exclude the queries that are very rare across all sessions
        -- to avoid recommending obscure queries
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

# As the put_multi method doesn't work with thousands of entities, we need to process
# them one by one, and without multiple threads it takes several hours.
with ThreadPoolExecutor(max_workers=100) as executor:
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
