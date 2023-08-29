from os import environ
from time import sleep
import pandas as pd
from datetime import datetime

from src.Kafka_connection import KafkaConnector
from src.Kafka_producer import KafkaProducer

from util import logger


# TODO: read data from outside the container
# DATA_PATH = environ("data_path")
# TOPIC = environ("topic_name")
# MSG_INTERVAL = int(environ("time_interval")) # seconds
# BROKERS = environ("brokers")
DATA_PATH = "data/intermittent-renewables-production-france_FILTERED.csv"
TOPIC = 'energy'
MSG_INTERVAL = 2
BROKER = 'kafka:29092'


def kafka_writer(data: pd.DataFrame, topic: str):
    try:
        start = datetime.now()
        logger.log_i("Write data to Kafka",
                     f"Start to write data to kafka topic: {topic}")

        kafka_producer = KafkaConnector.get_producer(
            brokers=BROKER,
            client_id="kfc_producer"
        )
        
        if kafka_producer:
            logger.log_i("Write data to Kafka",
                     f"Created kafka producer: {kafka_producer}")

            for _, r in data.iterrows():
                KafkaProducer.write(
                    connection=kafka_producer,
                    message=r.to_dict(),
                    topic=topic
                )
                sleep(MSG_INTERVAL)

            kafka_producer.close()
            logger.log_i("Write data to Kafka",
                     f"Produced all messages do kafka topic: {topic}")

        logger.log_i("Write data to Kafka",
                     f"Elapsed time {datetime.now()-start}")

    except Exception as e:
        logger.log_e("Write data to Kafka",
                     f"Error writing data to kafka topic: {topic}")


def get_data(path: str) -> pd.DataFrame:
    try:
        start = datetime.now()
        logger.log_i("Get data",
                     f"Start to get data from path: {path}")

        # read csv from path
        data = pd.read_csv(path)
        data.drop_duplicates(inplace=True)
        data.dropna(inplace=True)

        logger.log_i("Get data",
                     f"Finished getting data. Elapsed time {datetime.now()-start}")

    except Exception as e:
        logger.log_e("Get data",
                     f"Error reading data from path: {path}")
        return pd.DataFrame()

    return data


if __name__ == '__main__':
    data = get_data(path=DATA_PATH)
    kafka_writer(data=data, topic=TOPIC)
