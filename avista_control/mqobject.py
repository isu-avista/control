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

    def connect(self, host):
        """Connects to RabbitMQ server.

        Args:
            **host (str)**: host address
        """
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host))

    def create_channel(self):
        """Creates a channel on the connection."""
        self._channel = self._connection.channel()

    @abstractmethod
    def create_queue(self):
        pass
