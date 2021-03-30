from abc import ABC, abstractmethod
import pika


class MQObject(ABC):
    """Abstract message queuing class.

    Attributes:
        **_connection (object)**: pika connection to RabbitMQ server
        **_channel (object)**: connection channel
    """
    def __init__(self):
        """MQObject constructor."""
        self._connection = None
        self._channel = None

    def connect(self, url):
        """Connects to RabbitMQ server.

        Args:
            **url (str)**: rabbitmq connection url
        """
        self._connection = pika.BlockingConnection(
            pika.URLParameters(url))

    def create_channel(self):
        """Creates a channel on the connection."""
        self._channel = self._connection.channel()

    @abstractmethod
    def create_queue(self):
        pass
