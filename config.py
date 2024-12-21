from flask_caching import Cache

cache = Cache(config={"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300})
API_BASE_URL = "https://query-recommendations.uc.r.appspot.com"
TABLE = "query-recommendations.refined_cleaned_queries.refined_cleaned_queries"
CLEAN_PATTERN = r"^[-\*\s`#\[\]:\^/!\?;\.']+|[-\*\s`#\[\]:\^/!\?;\.']+$"
