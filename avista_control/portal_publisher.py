import uuid
import json
import pika
from avista_control.publisher import Publisher


class PortalPublisher(Publisher):
    """Message publishing client for sending tasks to mlearn."""

    def __init__(self):
        """Constructs a new PortalPublisher."""
        super().__init__()

    def call(self, task):
        """Publishes a message to consumer with task and data.

        Args:
            **task (dict)**: json dict

        Return:
            String with response in json format
        """
        self._response = None
        self._corr_id = str(uuid.uuid4())
        self._channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=pika.BasicProperties(
                reply_to=self._callback_queue,
                correlation_id=self._corr_id,
            ),
            body=json.dumps(task))

        while self._response is None:
            self._connection.process_data_events()

        return self._response

    def get_data(self):
        """Gets any uncollected data from avista portal"""
        pass

    def post_prediction_data(self, response):
        pass