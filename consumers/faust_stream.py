"""Defines trends calculations for stations"""
import logging
from dataclasses import dataclass, asdict

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("process-db-stations", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("org.chicago.cta.psql.stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations", partitions=1)
table = app.Table(
   "org.chicago.cta.stations.table",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)


@app.agent(topic)
async def process_station(stations):
    async for station in stations:
        line = "NA"
        if station.red:
            line = "red"
        elif station.blue:
            line = "blue"
        elif station.green:
            line = "green"

        transformed_station = TransformedStation(station.station_id,
                                                 station.station_name,
                                                 station.order,
                                                 line)
        logger.info(f"process_station: {asdict(transformed_station)}")
        table[transformed_station.station_id] = transformed_station


if __name__ == "__main__":
    app.main()
