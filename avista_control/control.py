from abc import ABC, abstractmethod
import logging
import functools
import pika

LOGGER = logging.getLogger(__name__)


class MQObject(ABC):
    EXCHANGE = 'message'
    EXCHANGE_TYPE = 'topic'
    QUEUE = 'text'
    ROUTING_KEY = 'example.text'

    def __init__(self, amqp_url):
        self._connection = None
        self._channel = None
        self._url = amqp_url

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.
        :rtype: pika.SelectConnection
        """
        LOGGER.info('Connecting to %s', self._url)
        return pika.SelectConnection(
            parameters=pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def on_connection_open(self, _unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.
        :param pika.SelectConnection _unused_connection: The connection
        """
        LOGGER.info('Connection opened')
        self.open_channel()

    @abstractmethod
    def on_connection_open_error(self, _unused_connection, err):
        pass

    @abstractmethod
    def on_connection_closed(self, _unused_connection, reason):
        pass

    def open_channel(self):
        """This method will open a new channel with RabbitMQ by issuing the
        Channel.Open RPC command. When RabbitMQ confirms the channel is open
        by sending the Channel.OpenOK RPC reply, the on_channel_open method
        will be invoked.
        """
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.
        Since the channel is now open, we'll declare the exchange to use.
        :param pika.channel.Channel channel: The channel object
        """
        LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.
        """
        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    @abstractmethod
    def on_channel_closed(self, channel, reason):
        pass

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.
        :param str|unicode exchange_name: The name of the exchange to declare
        """
        LOGGER.info('Declaring exchange: %s', exchange_name)
        # Note: using functools.partial is not required, it is demonstrating
        # how arbitrary data can be passed to the callback when it is called
        cb = functools.partial(
            self.on_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.EXCHANGE_TYPE,
            callback=cb)

    def on_exchange_declareok(self, _unused_frame, userdata):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.
        :param pika.Frame.Method _unused_frame: Exchange.DeclareOk response frame
        :param str|unicode userdata: Extra user data (exchange name)
        """
        LOGGER.info('Exchange declared: %s', userdata)
        self.setup_queue(self.QUEUE)

    @abstractmethod
    def setup_queue(self, queue_name):
        pass

    @abstractmethod
    def on_queue_declareok(self, _unused_frame):
        pass

    @abstractmethod
    def on_bindok(self, _unused_frame):
        pass

    def close_channel(self):
        """Invoke this command to close the channel with RabbitMQ by sending
        the Channel.Close RPC command.
        """
        if self._channel is not None:
            LOGGER.info('Closing the channel')
            self._channel.close()

    @abstractmethod
    def close_connection(self):
        pass


class Publisher(MQObject):

    def __init__(self, amqp_url):
        super().__init__(amqp_url)
        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None
        self._stopping = False

    def on_connection_open_error(self, _unused_connection, err):
        pass

    def on_connection_closed(self, _unused_connection, reason):
        pass

    def reconnect(self):
        pass

    def on_channel_closed(self, channel, reason):
        pass

    def setup_queue(self, queue_name):
        pass

    def on_queue_declareok(self, _unused_frame):
        pass

    def on_bindok(self, _unused_frame):
        pass

    def set_qos(self):
        pass

    def on_basic_qos_ok(self, _unused_frame):
        pass

    def start_consuming(self):
        pass

    def add_on_cancel_callback(self):
        pass

    def on_consumer_cancelled(self, method_frame):
        pass

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        pass

    def acknowledge_message(self, delivery_tag):
        pass

    def stop_consuming(self):
        pass

    def on_cancelok(self, _unused_frame, userdata):
        pass

    def close_connection(self):
        pass


class Consumer(MQObject):

    def __init_(self, amqp_url):
        super().__init__(amqp_url)
        self.should_reconnect = False
        self.was_consuming = False
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        self._prefetch_count = 1

    def on_connection_open_error(self, _unused_connection, err):
        pass

    def on_connection_closed(self, _unused_connection, reason):
        pass

    def on_channel_closed(self, channel, reason):
        pass

    def setup_queue(self, queue_name):
        pass

    def on_queue_declareok(self, _unused_frame):
        pass

    def on_bindok(self, _unused_frame):
        pass

    def start_publishing(self):
        pass

    def enable_delivery_confirmations(self):
        pass

    def on_delivery_confirmation(self, method_frame):
        pass

    def schedule_next_message(self):
        pass

    def publish_message(self):
        pass

    def close_connection(self):
        pass
