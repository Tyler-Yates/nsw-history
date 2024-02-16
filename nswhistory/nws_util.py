import datetime
from typing import List

import requests

from nswhistory.db_entry import DbEntry
from nswhistory.util import convert_c_to_f

LIMIT = 48
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def get_nws_temperatures(station_id: str) -> List[DbEntry]:
    temperatures = []

    url = f"https://api.weather.gov/stations/{station_id}/observations?limit={LIMIT}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()
    for feature in data["features"]:
        try:
            feature = _process_feature(feature)
            temperatures.append(feature)
        except Exception as e:
            print(f"ERROR! Exception processing feature {feature}: {e}")

    return temperatures


def _process_feature(feature: dict) -> DbEntry:
    timestamp = datetime.datetime.strptime(feature["properties"]["timestamp"], DATETIME_FORMAT)
    temperature_c = feature["properties"]["temperature"]["value"]
    temperature_f = convert_c_to_f(temperature_c)
    temperature_f = round(temperature_f, 4)

    return DbEntry(timestamp=timestamp, temp=temperature_f)
