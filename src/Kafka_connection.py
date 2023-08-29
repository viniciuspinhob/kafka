import json
from kafka import KafkaConsumer
from kafka import KafkaProducer

from util import logger


class KafkaConnector():
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
    def get_consumer(brokers: str, topic: str, client_id: str) -> KafkaConsumer:
        """
        Returns a kafka consumer, an instance to read messages from a kafka topic

        Args:
            brokers (str): list of kafka brokers which means ip address and 
                port of each one
            client_id (str): id of the consumer
            topic (str): the name of the topic from where messages are going
                to be consumed

        Returns:
            KafkaConsumer: an instance of Kafka Consumer
        """
        auto_offset_reset='earliest'
        enable_auto_commit=True

        try:
            connection = KafkaConsumer(
                topic,
                bootstrap_servers=brokers,
                client_id=client_id,
                group_id=client_id,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=enable_auto_commit,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            return connection
        except Exception as e:
            logger.log_e(
                "KafkaConnector", f"Error connecting to {brokers}/{topic}: {e}")

    @classmethod
    def get_producer(self, brokers: str, client_id: str, compression: str) -> KafkaProducer:
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
            connection = KafkaProducer(
                bootstrap_servers=brokers,
                client_id=client_id,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                compression_type=compression
            )
            return connection
        
        except Exception as e:
            logger.log_e(
                "KafkaConnector", f"Error connecting to {brokers}: {e}")
