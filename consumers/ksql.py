"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

KSQL_STATEMENT = """
CREATE TABLE ORG_CHICAGO_CTA_KSQL_TURNSTILE (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR,
    timestamp BIGINT
) WITH (
    KAFKA_TOPIC='org.chicago.cta.turnstiles.v1',
    VALUE_FORMAT='avro',
    KEY='timestamp'
);

CREATE TABLE ORG_CHICAGO_CTA_TURNSTILE_SUMMARY_V1
    WITH (VALUE_FORMAT='JSON') AS
        SELECT station_id, COUNT(*) AS count
            FROM ORG_CHICAGO_CTA_KSQL_TURNSTILE
            GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("ORG_CHICAGO_CTA_TURNSTILE_SUMMARY_V1") is True:
        logging.info('ksql turnstile summary table already exists')
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
