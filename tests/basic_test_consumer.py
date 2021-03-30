from avista_control.consumer import Consumer
import json
import pika


class BasicTestConsumer(Consumer):

    def on_request(self, ch, method, props, body):
        """Callback method send response after task execution.
        Args:
            **ch (object)**: pika channel object
            **method (object)**: pika method frame
            **props (object)**: pika properties class
            **body (str)**: json format string with task information
        """
        response = {'response': 'success'}

        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id=
                                                         props.correlation_id),
                         body=json.dumps(response))

        ch.basic_ack(delivery_tag=method.delivery_tag)
