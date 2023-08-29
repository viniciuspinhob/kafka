import json
from kafka import KafkaProducer

from util import logger

class KafkaWriter:
    """
    This class produces some messages in a given kafka topic.

    Methods:
        write(): Writes a message in a given kafka topic
    """

    @classmethod
    def write(self, connection: KafkaProducer, message: dict, topic: str) -> bool:
        """
        Writes a message to a Kafka topic.

        Args:
            connection (KafkaProducer): The kafka producer client.
            message (dict): The message to be written.
            topic (str): Topic where message should be written.

        Returns:
            bool: True if the write operation is successful, False otherwise.
        """
        try:
        
            # Send data to the Kafka topic
            connection.send(
                topic=topic,
                value=message
            )

            logger.log_i(f"kafka writer", f"Successfully wrote message to Kafka topic: {topic}")

        except Exception as e:
            logger.log_e('Kafka_producer', f'Write exception: {e}')

            return False

        return True
