import uuid
import json
import pika
import yaml
import requests
from avista_control.publisher import Publisher


class PortalPublisher(Publisher):
    """Message publishing client for sending tasks to mlearn."""

    def __init__(self):
        """Constructs a new PortalPublisher."""
        super().__init__()
        self.config = None
        self.load_config('./conf/config.yml')

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

    def load_config(self, file):
        with open(file, 'r') as f:
            self.config = yaml.load(f, Loader=yaml.FullLoader)

    def update_config(self, file):
        with open(file, 'w') as f:
            yaml.dump(self.config, f)

    def update_last_timestamp(self, ts):
        print(ts)
        self.config['last_ts'] = ts
        self.update_config('./conf/config.yml')

    def get_data(self):
        """Gets any uncollected data from avista portal"""
        response = requests \
            .get(f"http://{self.config['host']}:{self.config['port']}/api/data/{self.config['last_ts']}").json()

        oldest_timestamp = 0
        for point in response:
            if point[0] >= oldest_timestamp:
                oldest_timestamp = point[0]

        self.update_last_timestamp(oldest_timestamp)

    def post_prediction_data(self, response):
        """Posts prediction data to avista portal"""
        pass
