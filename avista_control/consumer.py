from abc import ABC, abstractmethod
from avista_control.mqobject import MQObject


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
