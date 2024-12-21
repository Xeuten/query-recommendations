import re

from flask import Blueprint, request
from google.cloud import datastore

from app.utils import make_cache_key
from config import CLEAN_PATTERN, cache

main = Blueprint("main", __name__)
client = datastore.Client()


@main.route("/related", methods=["GET"])
@cache.cached(key_prefix=make_cache_key)
def related():
    query = request.args.get("query") or ""
    cleaned_query = re.sub(CLEAN_PATTERN, "", query).lower()
    if not cleaned_query:
        return "Query is not present or is incorrect.", 400

    entity = client.get(client.key("TestQuery1", cleaned_query))
    if entity is None:
        return "No recommendations found for the given query.", 404
    return {"related": entity["related_queries"]}
