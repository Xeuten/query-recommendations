from flask import Blueprint, request
from google.cloud import datastore

from config import cache

main = Blueprint("main", __name__)
client = datastore.Client()


@main.route("/related", methods=["GET"])
@cache.cached(query_string=True)
def related():
    query = request.args.get("query")
    if query is None:
        return "The 'query' query parameter is required.", 400

    entity = client.get(client.key("Query", f"_{query}"))
    if entity is None:
        return "No recommendations found for the given query.", 404
    return str(entity["related_queries"])
