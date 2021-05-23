"""Contains functionality related to Weather"""
import logging

from confluent_kafka import Message

logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message: Message):
        """Handles incoming weather data"""
        weather_message = message.value()
        self.temperature = weather_message["temperature"]
        self.status = weather_message["status"]
