import json
from aiokafka import AIOKafkaProducer

from src.MessageBox import MessageBox
from src.KafkaConnector import KafkaConnector

from util import logger


class Kafka_producer():
    """
    This class produces some messages in a given kafka topic.

    Example of parameters:
    
        { 
        "brokers" : "",
        "topic" : "",
        "client_id" : "",
        "compression_type": ""
        }

    Attributes:
        description (str): the description of the writer
        connection (KafkaProducer): a Kafka producer instance
        topic (str): the name of the topic to be used

    Methods:
        write(): Writes a Message box in a given kafka topic
    """

    def __init__(self, brokers:str, client_id:str, compression:str, topic:str):
        """
        Initializes a Kafka writer.

        Args:
            
        """

        self.connection = None
        self.topic = topic
        self.brokers=brokers
        self.client_id=client_id
        self.compression=compression

    async def _connect_to_kafka(self):
        """
        Establishes a connection to Kafka.
        """
        try:
            connection = await KafkaConnector.get_producer(
                brokers=self.brokers,
                client_id=self.client_id,
                compression=self.compression
            )
            logger.log_i("kafka writer",
                         f"Established connection to Kafka {connection}")
        except Exception as e:
            logger.log_e("kafka writer",
                         f"Error connecting to producer {self.client_id}: {e}")

    async def write(self, msg_box: MessageBox) -> bool:
        """
        Writes a Message box to a Kafka topic.

        Args:
            msg_box (MessageBox): The Message box to be written.

        Returns:
            bool: True if the write operation is successful, False otherwise.
        """
        try:
            if self.connection is None:
                await self._connect_to_kafka()

            # Send data to the Kafka topic
            await self.connection.send_and_wait(self.topic, msg_box.get_json())
            logger.log_i(f"kafka writer", "Successfully wrote message to Kafka")

        except Exception as e:
            logger.log_e('Kafka_producer', f'{self.client_id} :: Write exception: {e}')

            return False

        return True
