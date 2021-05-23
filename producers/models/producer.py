"""Producer base-class providing common utilities and functionality"""
import logging
import time
from concurrent.futures import Future
from typing import Dict, Optional

from avro.schema import SchemaFromJSONData
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set()
    BROKER_URL: str = 'PLAINTEXT://localhost:9092'
    SCHEMA_REGISTRY_URL: str = 'http://localhost:8081'

    def __init__(self,
                 topic_name: str,
                 key_schema: SchemaFromJSONData,
                 value_schema: Optional[SchemaFromJSONData] = None,
                 num_partitions: int = 1,
                 num_replicas: int = 1,
                 ):
        """Initializes a Producer object with basic settings"""
        self.topic_name: str = topic_name
        self.key_schema: SchemaFromJSONData = key_schema
        self.value_schema: Optional[SchemaFromJSONData] = value_schema
        self.num_partitions: int = num_partitions
        self.num_replicas: int = num_replicas

        self.broker_properties: Dict[str, str] = {
            'bootstrap.servers': self.BROKER_URL
        }

        if self.topic_name not in Producer.existing_topics:
            try:
                self.create_topic()
                Producer.existing_topics.add(self.topic_name)
                logger.info(f'topic creation for {self.topic_name} successful')
            except Exception as e:
                logger.info(f'topic creation for {self.topic_name} failed: {e}')

        self.producer: AvroProducer = AvroProducer(config=self.broker_properties,
                                                   schema_registry=CachedSchemaRegistryClient(self.SCHEMA_REGISTRY_URL),
                                                   default_key_schema=self.key_schema,
                                                   default_value_schema=self.value_schema)

    def create_topic(self) -> None:
        """Creates the producer topic if it does not already exist"""
        client = AdminClient(self.broker_properties)
        futures: Dict[str, Future] = client.create_topics([NewTopic(topic=self.topic_name,
                                                                    num_partitions=self.num_partitions,
                                                                    replication_factor=self.num_replicas)])
        _ = [future.result() for future in futures.values()]

    def close(self) -> None:
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()
        logger.info(f'closing producer for topic {self.topic_name} complete')

    @staticmethod
    def time_millis() -> int:
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
