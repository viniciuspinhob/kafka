import json
from aiokafka import AIOKafkaProducer

from util import logger

class KafkaProducer:
    """
    This class produces some messages in a given kafka topic.

    Methods:
        write(): Writes a data package in a given kafka topic
    """

    @classmethod
    async def write(connection: AIOKafkaProducer, message: dict, topic: str) -> bool:
        """
        Writes a Message box to a Kafka topic.

        Args:
            connection (AIOKafkaProducer): The kafka producer client.
            message (dict): The message to be written.
            topic (str): Topic where message should be written.

        Returns:
            bool: True if the write operation is successful, False otherwise.
        """
        try:
        
            # Send data to the Kafka topic
            await connection.send_and_wait(topic, json.dumps(message))
            logger.log_i(f"kafka writer", "Successfully wrote message to Kafka")

        except Exception as e:
            logger.log_e('Kafka_producer', f'Write exception: {e}')

            return False

        return True
