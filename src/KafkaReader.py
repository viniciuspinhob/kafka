import datetime
import json
import pandas as pd
import asyncio
from aiokafka import AIOKafkaConsumer

from src.MessageBox import MessageBox
from src.KafkaConnector import KafkaConnector

from util import logger


class KafkaReader():
    """
    This class consumes some messages from a given kafka topic.

    Example of parameters:

        { 
            "read_time" : "", 
            "brokers" : "", 
            "topic" : "", 
            "client_id": ""
        }

    Attributes:
        name (str): the name of the reader
        description (str): the description of the reader
        connection (KafkaConsumer): a Kafka consumer instance

    Methods:
        read(): Reads data from a kafka topic
    """

    def __init__(self, name: str, description: str, parameters: str, items: pd.DataFrame):
        """
        Initializes a Kafka reader.

        Args:
            name (str): The name of the reader.
            description (str): The description of the reader.
            parameters (str): The JSON-encoded string containing reader parameters.
            items (pd.DataFrame): The DataFrame containing reader items.
        """
        self.parameters = json.dumps(parameters)
        self.parameters = json.loads(self.parameters)
        self.connection = None

    async def _connect_to_kafka(self):
        """
        Connects to a Kafka consumer.

        Returns:
            KafkaConsumer: The Kafka consumer instance.
        """
        return await KafkaConnector.get_consumer(
            brokers=self.parameters['brokers'],
            topic=self.parameters['topic'],
            client_id=self.parameters['client_id']
        )

    async def read(self) -> MessageBox:
        """
        Reads data from a Kafka topic.

        Yields:
            MessageBox: A MessageBox instance with the content of the message(s).
        """
        if self.connection is None:
            self.connection = await self._connect_to_kafka()

        if self.connection is not None:
            try:
                # initializes structures of Message box
                data = []
                metadata = {}

                timestamp = datetime.datetime.now(
                    tz=datetime.timezone.utc
                ).isoformat()

                msg_count = 0
                timeout = float(self.parameters['read_time'])
                messages_kafka = await self.connection.getmany(timeout_ms=timeout)
                for _, messages in messages_kafka.items():
                    for message in messages:
                        # Process message
                        data.append(message.value)
                        msg_count += 1
                logger.log_i(f'{self.name} : KAFKA_READER',
                             f'Read {msg_count} message from kafka topic')

                # add some fields to the metadata structurecls
                metadata['timestamp'] = timestamp
                # return a Message box
                yield MessageBox(data=data, metadata=metadata)

            except Exception as e:
                logger.log_ex(f'{self.name} : KAFKA_READER',
                              f'Read exception {e}')
        else:
            logger.log_w(f'{self.name} : KAFKA_READER',
                         'Consumer not initialized')
