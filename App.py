import asyncio
from os import environ
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
DATA_PATH = "intermittent-renewables-production-france_FILTERED.csv"
TOPIC = 'energy'
MSG_INTERVAL = 2
BROKERS = 'localhost:9092'



def main():
    data = get_data(path=DATA_PATH)
    kafka_writer(data=data, topic=TOPIC)


async def kafka_writer(data: pd.DataFrame, topic: str):
    kafka_producer = await KafkaConnector.get_producer(
        client_id=f"kfc_producer",
        compression="lz4"
    )

    for _, r in data.iterrows():
        KafkaProducer.write(
            connection=kafka_producer,
            message=r.to_dict(),
            topic=topic
        )
        await asyncio.sleep(MSG_INTERVAL)



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
   main()
