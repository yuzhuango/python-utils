from kombu import Connection, Consumer, Producer
from kombu.asynchronous import Hub


class KombuClient():
    """rabbitmq client base on celery.kombu"""

    def __init__(self, amqp_url):
        self._conn = Connection(amqp_url)
        self._hub = None

    def queue_declare(self, queue):
        """
        :param queue: kombu.queue obj
        :return: Returns a tuple containing 3 items:
            1. the name of the queue (essential for automatically-named queues),
            2. message count
            3. consumer count
        """
        channel = self._conn.channel()
        return queue.queue_declare(channel=channel)

    def produce(self, message, exchange, routing_key):
        """
        对于生产者而言，需要知道三个信息：
            1. 发送的exchange
            2. 分发的routing_key
            3. 发送的消息
        """
        producer = Producer(self._conn)
        producer.publish(
            body=message,
            exchange=exchange,
            routing_key=routing_key,
        )

    def consume(self, queue, callback):
        """
        对于消费者而言，需要知道两个信息：
            1. 从哪个queue消费
            2. 消费后回调函数
        """
        self._hub = Hub()
        self._conn.register_with_event_loop(self._hub)
        with Consumer(self._conn, [queue], on_message=callback):
            self._hub.run_forever()

    def close(self):
        """
        资源释放：
            1. 停止轮询
            2. 关闭连接
        """
        if self._hub:
            self._hub.stop()
        self._conn.close()



