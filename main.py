from websocket_to_kafka import main as websocket_main
from kafka_consumer import start_consumer
from globals import topic
if __name__ == "__main__":

    # Запускаем веб-сокеты и отправляем данные в Kafka
    websocket_main(topic)

    # Запускаем поток для чтения данных из Kafka
    start_consumer(topic, 'localhost:9092')

