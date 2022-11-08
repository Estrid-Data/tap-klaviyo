from pathlib import Path
from typing import Any, Optional

import json

from tap_klaviyo.client import KlaviyoStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class MetricsStream(KlaviyoStream):

    name = "metrics"
    path = "/metrics"
    primary_keys = ["id"]
    replication_key = "updated"

    schema_filepath = SCHEMAS_DIR / "metrics.json"

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row = super().post_process(row, context)

        # if row[self.replication_key] is greater than the bookmark, return row else return None

        return row

class SegmentsStream(KlaviyoStream):

    name = "segments"
    path = "/segments"
    primary_keys = ["id"]
    replication_key = "updated"

    schema_filepath = SCHEMAS_DIR / "segments.json"

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> dict[str, Any]:
        
        parent_params = super().get_url_params(context, next_page_token)
        
        params = {
            "filter": f"greater-than({self.replication_key},{self.get_starting_timestamp(context)})",
        }
        
        return parent_params | params


class EventsStream(KlaviyoStream):

    name = "events"
    path = "/events"
    primary_keys = ["id"]
    replication_key = "datetime"
    is_sorted = True

    schema_filepath = SCHEMAS_DIR / "events.json"

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row = super().post_process(row, context)

        row["event_properties"] = json.dumps(row["event_properties"])

        return row

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> dict[str, Any]:
        
        parent_params = super().get_url_params(context, next_page_token)
        
        params = {
            "filter": f"greater-than({self.replication_key},{self.get_starting_timestamp(context)})",
            "sort": self.replication_key
        }
        
        return parent_params | params

class ListsStream(KlaviyoStream):

    name = "lists"
    path = "/lists"
    primary_keys = ["id"]
    replication_key = "updated"

    schema_filepath = SCHEMAS_DIR / "lists.json"

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> dict[str, Any]:
        
        parent_params = super().get_url_params(context, next_page_token)
        
        params = {
            "filter": f"greater-than(updated,{self.get_starting_timestamp(context)})",
        }
        
        return parent_params | params

class FlowsStream(KlaviyoStream):

    name = "flows"
    path = "/flows"
    primary_keys = ["id"]
    replication_key = "updated"
    is_sorted = True

    schema_filepath = SCHEMAS_DIR / "flows.json"

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> dict[str, Any]:
        
        parent_params = super().get_url_params(context, next_page_token)
        
        params = {
            "filter": f"greater-than(updated,{self.get_starting_timestamp(context)})",
            "sort": self.replication_key
        }
        
        return parent_params | params

class ProfilesStream(KlaviyoStream):

    name = "profiles"
    path = "/profiles"
    primary_keys = ["id"]
    replication_key = "updated"

    schema_filepath = SCHEMAS_DIR / "profiles.json"

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row = super().post_process(row, context)

        row["properties"] = json.dumps(row["properties"])

        return row

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> dict[str, Any]:
        
        parent_params = super().get_url_params(context, next_page_token)
        
        params = {
            "filter": f"greater-than(updated,{self.get_starting_timestamp(context)})",
        }
        
        return parent_params | params