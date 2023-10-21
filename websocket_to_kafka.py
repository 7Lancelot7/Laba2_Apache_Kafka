from confluent_kafka import Producer
import json
from websockets.sync.client import connect
import time
# from kafka import KafkaProducer

from websockets.sync.client import connect
from kafka.admin import KafkaAdminClient, NewTopic




def send_to_kafka(message, producer, topic):
    producer.poll(0)
    producer.produce(topic, key=None, value=json.dumps(message))
    producer.flush()

def main(topic):
    # Define Kafka topic and server

    bootstrap_servers = 'localhost:9092'

    # Create Kafka producer
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    # producer = KafkaProducer(
    #     bootstrap_servers=bootstrap_servers,
    #     value_serializer=lambda v: json.dumps(v).encode('utf-8')
    # )
    channgel_name = "live_orders_btcusd"
    subscription_msg = {
        "event": "bts:subscribe",
        "data": {
            "channel": channgel_name
        }
    }
    unsubscription_msg = {
        "event": "bts:unsubscribe",
        "data": {
            "channel": channgel_name
        }
    }
    ws_url = "wss://ws.bitstamp.net"

    with connect(ws_url) as websocket:
        websocket.send(json.dumps(subscription_msg))
        message = websocket.recv()
        print(f"Received: {message}")

        print("-" * 20)

        transactions = []

        for _ in range(20):
            time.sleep(1)
            message = websocket.recv()
            print(f"Received: {message}")

            data = json.loads(message).get("data")
            if data and "price" in data:
                transactions.append(data)

        top_10_transactions = sorted(transactions, key=lambda x: x.get("price"), reverse=True)[:10]

        for transaction in top_10_transactions:
            send_to_kafka(transaction, producer, topic)

        print("-" * 20)

        websocket.send(json.dumps(unsubscription_msg))



if __name__ == "__main__":
    main()