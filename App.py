import asyncio
from os import environ
from uuid import uuid4
import pandas as pd
from datetime import datetime

from src.Kafka_producer import *
import MessageBox

from util import logger


DATA_SOURCE = environ("data_source")
DATA_PATH = environ("data_path")
TOPIC = environ("topic_name")
BROKERS = "docker.host.internal:9092"


async def kafka_writer(source: str, data):

    if source == 'internal':
        # Init kafka producers
        KWriter_wind = Kafka_producer(
                brokers = BROKERS,
                topic = "wind_energy_source",
                client_id = "KWriter_wind",
                compression_type = "lz4"
        )
        for _, r in data['wind']:
            KWriter_wind.write(
                MessageBox(
                    data=r.to_dict,
                    metadata={
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
            )

        # KWriter_solar = kafka_producer.connect_to_kafka(
        #     parameters = {
        #         'brokers': BROKER,
        #         'topic': "wind_energy_source",
        #         'client_id': "KWriter_solar",
        #         "compression_type": "lz4"
        #     }
        # )

        # iterate over data and send messages to kafka broker

    # else:
    #     KWriter_general = kafka_producer.connect_to_kafka(
    #         parameters = {
    #             'brokers': BROKER,
    #             'topic': TOPIC if TOPIC else 'generic_topic',
    #             'client_id': "KWriter_general",
    #             "compression_type": "lz4"
    #         }
    #     )
        
    #     pass

    return True


def get_data(source: str, path: str) -> dict:
    start = datetime.now()
    logger.log_i(
            "Get data", f"Start to get data from source: {source} and path: {path}")
    data = {}

    if source == "internal":
        df = pd.read_csv('data/intermittent-renewables-production-france_FILTERED.csv') 
        
        # Split data by energy source 
        data['wind'] = df[df['Source'] == 'Wind']
        data['solar'] = df[df['Source'] == 'Solar']


    elif source == "external" and path:
        # read csv from path
        pass

    else:
        logger.log_e(
            "Get data", f"Bad data source or path. Data source: {source}, path: {path}")
    
    logger.log_i(
            "Get data", f"Finished getting data. Elapsed time {datetime.now()-start}")
    return data


if __name__ == 'main':
    data = get_data(DATA_SOURCE, DATA_PATH)
    kafka_writer(DATA_SOURCE, data)
