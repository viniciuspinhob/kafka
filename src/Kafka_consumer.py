import asyncio
from aiokafka import AIOKafkaConsumer

from util import logger


class KafkaReader:
    """
    This class consumes some messages from a given kafka topic.

    Methods:
        read(): Reads data from a kafka topic
    """

    @classmethod
    async def read(connection: AIOKafkaConsumer, topic: str, read_time: float) -> dict:
        """
        Reads data from a Kafka topic.

        Yields:
            Dict with the content of the message(s).
        """
        try:
            data = {}
            msg_count = 0
            messages_kafka = await connection.getmany(timeout_ms = read_time)

            for _, messages in messages_kafka.items():
                for message in messages:
                    data.append(message.value)
                    msg_count += 1

            logger.log_i('KAFKA_READER',
                            f'Read {msg_count} message from kafka topic {topic}')


            # return a Message box
            yield data

        except Exception as e:
            logger.log_e('KAFKA_READER',
                            f'Read exception {e}')
