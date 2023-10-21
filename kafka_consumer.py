from confluent_kafka import Consumer
from globals import topic
from kafka.admin import KafkaAdminClient, NewTopic

def create_topic_if_not_exists(bootstrap_servers, topic):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topic_list = admin_client.list_topics()
    topic_names = [topic.topic for topic in iter(topic_list.topics.values())]

    if topic not in topic_names:
        admin_client.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])

def start_consumer(topic, bootstrap_servers):
    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(timeout=1000)
            if msg is None:
                continue
            if msg.error():
                print(f"Ошибка при получении сообщения: {msg.error()}")
                continue

            print(f"Получено сообщение: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":

    start_consumer(topic, 'localhost:9092')