from pathlib import Path
from typing import Any, Optional

import pendulum
import logging

from singer_sdk.helpers._typing import is_datetime_type
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIAuthenticatorBase, APIKeyAuthenticator
from singer_sdk.pagination import BaseAPIPaginator
from tap_klaviyo.paginator import KlaviyoPaginator

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
API_VERSION = "2022-10-17"

class KlaviyoStream(RESTStream):
    
    url_base = "https://a.klaviyo.com/api"

    records_jsonpath: str = "$.data[*]"
    next_page_token_jsonpath: str = "$.links.next"

    @property
    def authenticator(self) -> APIAuthenticatorBase:
        return APIKeyAuthenticator(self, "Authorization", f"Klaviyo-API-Key {self.config['api_key']}")

    def get_new_paginator(self) -> BaseAPIPaginator:
        return KlaviyoPaginator(self.next_page_token_jsonpath)

    @property
    def http_headers(self) -> dict:
        result = self._http_headers

        if "user_agent" in self.config:
            result["User-Agent"] = self.config.get("user_agent")

        result["revision"] = API_VERSION

        return result

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> dict[str, Any]:

        return {"page[cursor]": next_page_token} if next_page_token else {}

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        
        for key, value in row["attributes"].items():
            row[key] = value

        del row["attributes"]

        return row