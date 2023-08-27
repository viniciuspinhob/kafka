import json
import asyncio
from aiokafka import AIOKafkaConsumer
from aiokafka import AIOKafkaProducer

from util import logger


class KafkaConnector:
    """
    The KafkaConnector (static) class is factory responsible for connecting 
    to a kafka broker/topic. It has consumer and producer pools so in case 
    a new connection is requested and it already exists in the pool the existent 
    one is delivered.

    Methods:
        get_consumer(): 
            Returns a kafka consumer, an instance to read messages from a kafka topic
        get_producer(): 
            Returns a kafka producer, an instance to write messages to a kafka topic
    """

    # static attributes
    consumer_pool = {}
    producer_pool = {}

    @classmethod
    async def get_consumer(cls, brokers: str, topic: str, client_id: str) -> AIOKafkaConsumer:
        """
        Returns a kafka consumer, an instance to read messages from a kafka topic

        Args:
            brokers (str): list of kafka brokers which means ip address and 
                port of each one
            topic (str): the name of the topic from where messages are going
                to be consumed

        Returns:
            KafkaConsumer: an instance of Kafka Consumer
        """
        # set the connection key
        connection_key = f'{brokers}/{topic}'
        auto_offset_reset='earliest'
        enable_auto_commit=True
        # check if there is a connection in the pool
        if connection_key in cls.consumer_pool.keys():
            # TO DO Check the connection status, validate it before return

            # return an existent connection
            return cls.consumer_pool[connection_key]
        else:
            try:
                # return a new connection
                connection = AIOKafkaConsumer(
                    topic,
                    bootstrap_servers=brokers,
                    client_id=client_id,
                    group_id=client_id,
                    auto_offset_reset=auto_offset_reset,
                    enable_auto_commit=enable_auto_commit,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
                # add to the connection pool
                cls.consumer_pool[connection_key] = connection
                await connection.start()
                return connection
            except Exception as e:
                logger.log_e(
                    "KafkaConnector", f"Error connecting to {brokers}/{topic}: {e}")

    @classmethod
    async def get_producer(cls, brokers: str, client_id: str, compression: str = None) -> AIOKafkaProducer:
        """
        Returns a kafka producer, an instance to write messages to a kafka topic

        Args:
            brokers (str): list of kafka brokers which means ip address and 
                port of each one

        Returns:
            KafkaProducer: an instance of Kafka Producer
        """
        # set the connection key
        connection_key = f'{brokers}'
        # check if there is a connection in the pool
        if connection_key in cls.producer_pool.keys():
            # TODO Check the connection status, validate it before return

            # return an existent connection
            return cls.producer_pool[connection_key]
        else:
            try:
                # return a new connection
                connection = AIOKafkaProducer(
                    bootstrap_servers=brokers,
                    client_id=client_id,
                    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                    compression_type= compression
                )
                # add to the connection pool
                cls.producer_pool[connection_key] = connection
                await connection.start()
            except Exception as e:
                logger.log_e(
                    "KafkaConnector", f"Error connecting to {brokers}: {e}")
