from abc import ABC, abstractmethod
import json
import logging
import functools
import pika


LOGGER = logging.getLogger(__name__)


class MQObject(ABC):
    EXCHANGE = 'message'
    EXCHANGE_TYPE = 'topic'
    PUBLISH_INTERVAL = 1
    QUEUE = 'text'
    ROUTING_KEY = 'example.text'

    def __init__(self, amqp_url):
        self._connection = None
        self._channel = None
        self._url = amqp_url

    def connect(self):
        print(f"Connecting to {self._url}")
        return pika.SelectConnection(
            parameters=pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def on_connection_open(self, _unused_connection):
        print('Connection opened')
        self.open_channel()

    @abstractmethod
    def on_connection_open_error(self, _unused_connection, err):
        pass

    @abstractmethod
    def on_connection_closed(self, _unused_connection, reason):
        pass

    def open_channel(self):
        print('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        print('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        print('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    @abstractmethod
    def on_channel_closed(self, channel, reason):
        pass

    def setup_exchange(self, exchange_name):
        print(f"Declaring exchange: {exchange_name}")
        # Note: using functools.partial is not required, it is demonstrating
        # how arbitrary data can be passed to the callback when it is called
        cb = functools.partial(
            self.on_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.EXCHANGE_TYPE,
            callback=cb)

    def on_exchange_declareok(self, _unused_frame, userdata):
        print(f"Exchange declared: {userdata}")
        self.setup_queue(self.QUEUE)

    @abstractmethod
    def setup_queue(self, queue_name):
        pass

    def close_channel(self):
        if self._channel is not None:
            print('Closing the channel')
            self._channel.close()

    @abstractmethod
    def close_connection(self):
        pass


class Publisher(MQObject, ABC):

    def __init__(self, amqp_url):
        super().__init__(amqp_url)
        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None
        self._stopping = False

    def run(self):
        while not self._stopping:
            self._connection = None
            self._deliveries = []
            self._acked = 0
            self._nacked = 0
            self._message_number = 0

            try:
                self._connection = self.connect()
                self._connection.ioloop.start()
            except KeyboardInterrupt:
                self.stop()
                if (self._connection is not None and
                        not self._connection.is_closed):
                    # Finish closing
                    self._connection.ioloop.start()

        print('Stopped')

    def on_connection_open_error(self, _unused_connection, err):
        print(f"Connection open failed, reopening in 5 seconds: {err}")
        self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            print(f"Connection closed, reopening in 5 seconds: {reason}")
            self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def on_channel_closed(self, channel, reason):
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            print(f"Connection closed, reopening in 5 seconds: {reason}")
            self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def setup_queue(self, queue_name):
        print(f"Declaring queue: {queue_name}")
        self._channel.queue_declare(
            queue=queue_name, callback=self.on_queue_declareok)

    def on_queue_declareok(self, _unused_frame):
        print(f"Binding {self.EXCHANGE} to {self.QUEUE} with {self.ROUTING_KEY}")
        self._channel.queue_bind(
            self.QUEUE,
            self.EXCHANGE,
            routing_key=self.ROUTING_KEY,
            callback=self.on_bindok)

    def on_bindok(self, _unused_frame):
        print('Queue bound')
        self.start_publishing()

    def start_publishing(self):
        print('Issuing consumer related RPC commands')
        self.enable_delivery_confirmations()
        self.schedule_next_message()

    def enable_delivery_confirmations(self):
        print('Issuing Confirm.Select RPC command')
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        print(f"Received {confirmation_type} for delivery tag: {method_frame.method.delivery_tag}")
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
        print(
            f"Published {self._message_number} messages, {len(self._deliveries)} have yet to be confirmed, "
            f"{self._acked} were acked and {self._nacked} were nacked"
        )

    def schedule_next_message(self):
        print(f"Scheduling next message for {self.PUBLISH_INTERVAL} seconds")
        self._connection.ioloop.call_later(self.PUBLISH_INTERVAL,
                                           self.publish_message)

    def publish_message(self):
        if self._channel is None or not self._channel.is_open:
            return

        hdrs = {u'مفتاح': u' قيمة', u'键': u'值', u'キー': u'値'}
        properties = pika.BasicProperties(
            app_id='example-publisher',
            content_type='application/json',
            headers=hdrs)

        message = u'hello'
        self._channel.basic_publish(self.EXCHANGE, self.ROUTING_KEY,
                                    json.dumps(message, ensure_ascii=False),
                                    properties)
        self._message_number += 1
        self._deliveries.append(self._message_number)
        print(f"Published message # {self._message_number}")
        #self.schedule_next_message()

    def close_connection(self):
        if self._connection is not None:
            print('Closing connection')
            self._connection.close()

    def stop(self):
        print('Stopping')
        self._stopping = True
        self.close_channel()
        self.close_connection()


class Consumer(MQObject, ABC):

    def __init__(self, amqp_url):
        super().__init__(amqp_url)
        self.should_reconnect = False
        self.was_consuming = False
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        self._prefetch_count = 1

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def on_connection_open_error(self, _unused_connection, err):
        print(f"Connection open failed: {err}")
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            print(f"Connection closed, reconnect necessary: {reason}")
            self.reconnect()

    def reconnect(self):
        self.should_reconnect = True
        self.stop()

    def on_channel_closed(self, channel, reason):
        print(f"Channel {channel} was closed: {reason}")
        self.close_connection()

    def setup_queue(self, queue_name):
        print(f"Declaring queue: {queue_name}")
        cb = functools.partial(self.on_queue_declareok, userdata=queue_name)
        self._channel.queue_declare(queue=queue_name, callback=cb)

    def on_queue_declareok(self, _unused_frame, userdata):
        queue_name = userdata
        print(f"Binding {self.EXCHANGE} to {queue_name} with {self.ROUTING_KEY}")
        cb = functools.partial(self.on_bindok, userdata=queue_name)
        self._channel.queue_bind(
            queue_name,
            self.EXCHANGE,
            routing_key=self.ROUTING_KEY,
            callback=cb)

    def on_bindok(self, _unused_frame, userdata):
        print(f"Queue bound: {userdata}")
        self.set_qos()

    def set_qos(self):
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        print(f"QOS set to: {self._prefetch_count}")
        self.start_consuming()

    def start_consuming(self):
        print('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self.QUEUE, self.on_message)
        self.was_consuming = True
        self._consuming = True

    def add_on_cancel_callback(self):
        print('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        print(f"Consumer was cancelled remotely, shutting down: {method_frame}")
        if self._channel:
            self._channel.close()

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        print(f"Received message # {basic_deliver.delivery_tag} from {properties.app_id}: {body}")
        self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        print(f"Acknowledging message {delivery_tag}")
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        if self._channel:
            print('Sending a Basic.Cancel RPC command to RabbitMQ')
            cb = functools.partial(
                self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

    def on_cancelok(self, _unused_frame, userdata):
        self._consuming = False
        print(f"RabbitMQ acknowledged the cancellation of the consumer: {userdata}")
        self.close_channel()

    def close_channel(self):
        print('Closing the channel')
        self._channel.close()

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            print('Connection is closing or already closed')
        else:
            print('Closing connection')
            self._connection.close()

    def stop(self):
        if not self._closing:
            self._closing = True
            print('Stopping')
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            print('Stopped')
