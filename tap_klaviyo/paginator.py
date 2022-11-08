from __future__ import annotations
from requests import Response
from urllib.parse import urlparse, parse_qs

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import JSONPathPaginator


class KlaviyoPaginator(JSONPathPaginator):

    def get_next(self, response: Response) -> str | None:
        next_page_url = next(extract_jsonpath(self._jsonpath, response.json()))

        if next_page_url is None:
            return None

        parameters = urlparse(next_page_url).query
        return parse_qs(parameters)["page[cursor]"][0]