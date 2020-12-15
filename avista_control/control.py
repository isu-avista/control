from abc import ABC, abstractmethod
import pika


class MQObject(ABC):
    EXCHANGE = 'message'
    EXCHANGE_TYPE = 'topic'
    PUBLISH_INTERVAL = 1
    QUEUE = 'text'
    ROUTING_KEY = 'example.text'

    def __init__(self):
        self._connection = None
        self._channel = None

    def connect(self, host):
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host))

    def create_channel(self):
        self._channel = self._connection.channel()

    @abstractmethod
    def create_queue(self):
        pass


class Consumer(MQObject, ABC):
    def __init__(self):
        super().__init__()

    def create_queue(self):
        self._channel.queue_declare(queue='rpc_queue')

    # This method will execute once a task is received
    # and then send the result back to the publisher
    @abstractmethod
    def on_request(self, ch, method, props, body):
        pass

    def start_consuming(self):
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue='rpc_queue', on_message_callback=self.on_request)
        self._channel.start_consuming()


class Publisher(MQObject, ABC):

    def __init__(self):
        super().__init__()
        self._callback_queue = None
        self._response = None
        self._corr_id = None

    def create_queue(self):
        result = self._channel.queue_declare(queue='', exclusive=True)
        self._callback_queue = result.method.queue

    # This is for result messages
    def declare_consumption(self):
        self._channel.basic_consume(
            queue=self._callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self._corr_id == props.correlation_id:
            self._response = body

    # This method will send a task to the consumer
    @abstractmethod
    def call(self, task):
        pass
