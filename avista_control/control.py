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


class Consumer(MQObject, ABC):
    """Abstract consumer class."""

    def __init__(self):
        """Consumer constructor."""
        super().__init__()

    def create_queue(self):
        """Creates a queue on the connection channel."""
        self._channel.queue_declare(queue='rpc_queue')

    @abstractmethod
    def on_request(self, ch, method, props, body):
        pass

    def start_consuming(self):
        """Starts consuming on the connection channel"""
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue='rpc_queue', on_message_callback=self.on_request)
        self._channel.start_consuming()


class Publisher(MQObject, ABC):
    """Abstract publisher class.

    Attributes:
        **_callback_queue (object)**: queue for response messages
        **_response (str)**: json string containing response from consumer
        **_corr_id (int)**: message correlation uuid
    """

    def __init__(self):
        """Publisher constructor."""
        super().__init__()
        self._callback_queue = None
        self._response = None
        self._corr_id = None

    def create_queue(self):
        """Declares a queue on the connection channel."""
        result = self._channel.queue_declare(queue='', exclusive=True)
        self._callback_queue = result.method.queue

    def declare_consumption(self):
        """Allows consumption of messages returned from the consumer."""
        self._channel.basic_consume(
            queue=self._callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        """Stores response after received and breaks consumption loop.

        Args:
            **ch (object)**: connection channel
            **method (object)**: method frame
            **props (object)**: pika properties
            **body (str)**: json string response
        """
        if self._corr_id == props.correlation_id:
            self._response = body

    @abstractmethod
    def call(self, task):
        pass
