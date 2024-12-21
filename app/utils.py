import re

from flask import request

from config import CLEAN_PATTERN


def make_cache_key(*args, **kwargs):
    query = request.args.get("query") or ""
    return re.sub(CLEAN_PATTERN, "", query).lower()
