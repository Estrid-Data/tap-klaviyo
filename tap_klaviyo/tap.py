from singer_sdk import Tap, Stream
from singer_sdk import typing as th

from tap_klaviyo.streams import (
    MetricsStream,
    EventsStream,
)

STREAM_TYPES = [
    MetricsStream,
    EventsStream,
]

class TapKlaviyo(Tap):
    """TapKlaviyo: Tap for extracting metrics from the Klaviyo API"""
    name = "tap-klaviyo"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="The start date for the tap to begin pulling data from when no bookmark is available"
        ),
        th.Property(
            "user_agent",
            th.StringType,
            required=True
        ),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
