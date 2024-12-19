from google.cloud import bigquery, datastore


bigquery_client = bigquery.Client()
datastore_client = datastore.Client()


table_id = "query-recommendations.sample_queries.queries"
query = f"""
    WITH session_queries AS (
        SELECT Session_ID, ARRAY_AGG(DISTINCT Query) AS queries
        FROM {table_id}
        GROUP BY Session_ID
    ),
    query_pairs AS (
        SELECT
            q1 AS query_1,
            q2 AS query_2,
            COUNT(*) AS cooccurrence_count
        FROM session_queries,
        UNNEST(queries) AS q1,
        UNNEST(queries) AS q2
        WHERE q1 != q2
        GROUP BY q1, q2
    ),
    top_k_related AS (
        SELECT
            query_1,
            ARRAY_AGG(query_2 ORDER BY cooccurrence_count DESC LIMIT 3) AS related_queries
        FROM query_pairs
        GROUP BY query_1
    )
    SELECT * FROM top_k_related
"""

bigquery_results = bigquery_client.query(query).result()
for row in bigquery_results:
    query_key = row["query_1"]
    related_queries = row["related_queries"]
    entity = datastore.Entity(datastore_client.key("Query", f"_{query_key}"))
    entity["related_queries"] = related_queries
    datastore_client.put(entity)
