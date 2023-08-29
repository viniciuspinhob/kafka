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

    @classmethod
    async def get_consumer(brokers: str, topic: str, client_id: str) -> AIOKafkaConsumer:
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
        auto_offset_reset='earliest'
        enable_auto_commit=True

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
            await connection.start()
            return connection
        except Exception as e:
            logger.log_e(
                "KafkaConnector", f"Error connecting to {brokers}/{topic}: {e}")

    @classmethod
    async def get_producer(brokers: str, client_id: str, compression: str = None) -> AIOKafkaProducer:
        """
        Returns a kafka producer, an instance to write messages to a kafka topic

        Args:
            brokers (str): list of kafka brokers which means ip address and 
                port of each one
            client_id (str): id of the producer
            compression (str): compression type

        Returns:
            KafkaProducer: an instance of Kafka Producer
        """
        try:
            # return a new connection
            connection = AIOKafkaProducer(
                bootstrap_servers=brokers,
                client_id=client_id,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                compression_type= compression
            )

            await connection.start()
        except Exception as e:
            logger.log_e(
                "KafkaConnector", f"Error connecting to {brokers}: {e}")
