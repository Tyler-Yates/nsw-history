import sys
from typing import List

import pytz
import requests
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.server_api import ServerApi

from nswhistory.config import _get_config
from nswhistory.nws_util import get_nws_temperatures

MONGO_DATABASE = "sensors"
MONGO_COLLECTION = "pitemp"
MONGO_GRANULARITY = "minutes"

META_FIELD = "sensorId"
TIMESTAMP_FIELD = "timestamp"
TEMPERATURE_FIELD = "temp_f"

CONFIG = _get_config()
TIMEZONE = pytz.timezone(CONFIG.timezone)


def _get_mongo_client() -> MongoClient:
    uri = (f"mongodb+srv://{CONFIG.mongo_username}:{CONFIG.mongo_password}@{CONFIG.mongo_host}"
           f"/?retryWrites=true&w=majority")
    # Create a new client and connect to the server
    return MongoClient(uri, server_api=ServerApi('1'))


def _ensure_mongo_setup(mongo_client: MongoClient) -> Collection:
    # Ensure we can connect
    try:
        mongo_client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
        sys.exit(1)

    # Ensure collection is set up
    database = mongo_client[MONGO_DATABASE]

    collection_exists = MONGO_COLLECTION in list(x["name"] for x in database.list_collections())
    if not collection_exists:
        database.create_collection(
            MONGO_COLLECTION,
            timeseries={
                "timeField": TIMESTAMP_FIELD,
                "metaField": META_FIELD,
                "granularity": MONGO_GRANULARITY,
            }
        )

    collection = database[MONGO_COLLECTION]
    return collection


def _ping_healthcheck():
    response = requests.get(CONFIG.healthcheck_url)
    response.raise_for_status()
    print("Pinged healthcheck")


def _ensure_only_one_entry(collection: Collection, entries: List):
    for i in range(len(entries) - 1):
        existing_entry = entries[i]
        try:
            collection.delete_one({"_id": existing_entry["_id"]})
        except Exception as e:
            print(f"Could not delete entry {existing_entry}: {e}")


def _save_nws_data():
    mongo_client = _get_mongo_client()
    collection = _ensure_mongo_setup(mongo_client)

    entries = get_nws_temperatures(CONFIG.station_id)

    for entry in entries:
        existing_entries = list(collection.find({TIMESTAMP_FIELD: entry.timestamp, META_FIELD: CONFIG.station_id}))
        if existing_entries:
            _ensure_only_one_entry(collection, existing_entries)

            print("Skipping document that already exists")
            continue

        collection.insert_one({
            TIMESTAMP_FIELD: entry.timestamp,
            META_FIELD: CONFIG.station_id,
            TEMPERATURE_FIELD: entry.temp,
        })
        print(f"Inserted document {entry} to Mongo.")


def main():
    _save_nws_data()
    _ping_healthcheck()


if __name__ == '__main__':
    main()
