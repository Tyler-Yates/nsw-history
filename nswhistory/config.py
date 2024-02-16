import json
import os
from dataclasses import dataclass
from pathlib import Path

CURRENT_FILE_PATH = Path(__file__).parent.absolute()


@dataclass
class Config:
    mongo_username: str
    mongo_password: str
    mongo_host: str
    station_id: str
    healthcheck_url: str
    timezone: str


def _get_config() -> Config:
    with open(os.path.join(CURRENT_FILE_PATH, "..", "config.json"), mode="r") as config_file:
        config = json.load(config_file)
        return Config(
            mongo_username=config["mongo_username"],
            mongo_password=config["mongo_password"],
            mongo_host=config["mongo_host"],
            station_id=config["station_id"],
            healthcheck_url=config["healthcheck_url"],
            timezone=config["timezone"],
        )
