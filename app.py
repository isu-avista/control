from avista_control.portal_publisher import PortalPublisher
from threading import Thread
import time

publisher = PortalPublisher()
publisher.connect('localhost')
publisher.create_channel()
publisher.create_queue()
publisher.declare_consumption()


def run_publisher():
    while True:
        # get data from portal if any
        data = publisher.get_data()

        # send message with data if any
        response = publisher.call(data)

        # post result back to portal
        publisher.post_prediction_data(response)

        # sleep
        time.sleep(float(publisher.config['periodicity']))


if __name__ == '__main__':
    pub_thread = Thread(target=run_publisher)
    pub_thread.start()
