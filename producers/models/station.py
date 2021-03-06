"""Methods pertaining to loading and configuring CTA "L" station data."""
from enum import Enum
import logging
from pathlib import Path
from typing import Optional

from avro.schema import SchemaFromJSONData
from confluent_kafka import avro

from models import Turnstile
from models.producer import Producer
from models.train import Train

logger = logging.getLogger(__name__)


class Station(Producer):
    """Defines a single station"""

    key_schema: SchemaFromJSONData = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_key.json")
    value_schema: SchemaFromJSONData = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_value.json")

    def __init__(self,
                 station_id: str,
                 name: str,
                 color: Enum,
                 direction_a: Optional['Station'] = None,
                 direction_b: Optional['Station'] = None):
        self.name = name
        station_name = (
            self.name.lower()
                .replace("/", "_and_")
                .replace(" ", "_")
                .replace("-", "_")
                .replace("'", "")
                .replace(".", "_")
        )

        topic_name = f"org.chicago.cta.station.{station_name}.arrivals.v1"
        super().__init__(
            topic_name,
            key_schema=Station.key_schema,
            value_schema=Station.value_schema,
            num_partitions=1,
            num_replicas=1
        )

        self.station_id: int = int(station_id)
        self.color: Enum = color
        self.dir_a: Optional['Station'] = direction_a
        self.dir_b: Optional['Station'] = direction_b
        self.a_train: Optional[Train] = None
        self.b_train: Optional[Train] = None
        self.turnstile: Turnstile = Turnstile(self)

    def run(self, train: Train, direction: str, prev_station_id: int, prev_direction: str) -> None:
        """Simulates train arrivals at this station"""
        logger.debug(f'Running station with id: {self.station_id} and for topic: {self.topic_name}')
        self.producer.produce(
            topic=self.topic_name,
            key={"timestamp": self.time_millis()},
            value={
                "station_id": self.station_id,
                "train_id": train.train_id,
                "train_status": train.status.name,
                "direction": direction,
                "line": self.color.name,
                "prev_station_id": prev_station_id,
                "prev_direction": prev_direction
            }
        )

    def __str__(self) -> str:
        return "Station | {:^5} | {:<30} | Direction A: | {:^5} | departing to {:<30} | Direction B: | {:^5} | departing to {:<30} | ".format(
            self.station_id,
            self.name,
            self.a_train.train_id if self.a_train is not None else "---",
            self.dir_a.name if self.dir_a is not None else "---",
            self.b_train.train_id if self.b_train is not None else "---",
            self.dir_b.name if self.dir_b is not None else "---",
        )

    def __repr__(self) -> str:
        return str(self)

    def arrive_a(self, train: Train, prev_station_id: int, prev_direction: str) -> None:
        """Denotes a train arrival at this station in the 'a' direction"""
        self.a_train = train
        self.run(train, "a", prev_station_id, prev_direction)

    def arrive_b(self, train: Train, prev_station_id: int, prev_direction: str) -> None:
        """Denotes a train arrival at this station in the 'b' direction"""
        self.b_train = train
        self.run(train, "b", prev_station_id, prev_direction)

    def close(self) -> None:
        """Prepares the producer for exit by cleaning up the producer"""
        self.turnstile.close()
        super(Station, self).close()
