from avista_control.portal_publisher import PortalPublisher

if __name__ == '__main__':
    publisher = PortalPublisher()
    publisher.connect('localhost')
    publisher.create_channel()
    publisher.create_queue()
    publisher.declare_consumption()

    while True:
        # get data from portal if any
        # publisher.get_data()

        # send message with data if any
        # publisher.call(data)

        # post result back to portal
        # publisher.post_prediction_data(response)
