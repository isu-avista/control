import unittest
from avista_control.portal_publisher import PortalPublisher
from tests.basic_test_consumer import BasicTestConsumer
import threading
import time


class PortalPublisherTest(unittest.TestCase):

    def setUp(self):
        self.url = 'amqp://pifpaf:secret@localhost:5682/%2F'
        self.publisher = PortalPublisher()
        self.consumer = BasicTestConsumer()
        self.consumer_thread = None
        self.publisher_thread = None
        self.response = None

    def tearDown(self):
        if self.publisher._connection and self.publisher._connection.is_open:
            self.publisher._connection.close()

        if self.consumer._connection and self.consumer._connection.is_open:
            self.consumer._connection.close()

    def setup_consumer(self):
        self.consumer.connect(self.url)
        self.consumer.create_channel()
        self.consumer.create_queue()
        self.consumer.start_consuming()

    def setup_publisher(self):
        self.publisher.connect(self.url)
        self.publisher.create_channel()
        self.publisher.create_queue()
        self.publisher.declare_consumption()

    def send_message(self):
        data = {'message': 'value'}
        self.setup_publisher()
        self.response = self.publisher.call(data)
        self.consumer._channel.stop_consuming()

    def test_publisher_setup(self):
        self.setup_publisher()

    def test_publisher_message(self):
        self.consumer_thread = threading.Thread(target=self.setup_consumer)
        self.consumer_thread.start()
        self.publisher_thread = threading.Thread(target=self.send_message)
        self.publisher_thread.start()
        self.consumer_thread.join()
        self.publisher_thread.join()
        self.assertEqual('success', self.response['response'])
