from websocket_to_kafka import main as websocket_main
from kafka_consumer import start_consumer

if __name__ == "__main__":
    topic="topic_bit_transactions"
    # Запускаем веб-сокеты и отправляем данные в Kafka
    websocket_main(topic)

    # Запускаем поток для чтения данных из Kafka
    start_consumer(topic, 'localhost:9092')

