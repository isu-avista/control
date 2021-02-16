from abc import ABC, abstractmethod
from mqobject import MQObject


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
