import json
from confluent_kafka import Consumer, KafkaError
import threading
import time

KAFKA_BOOTSTRAP_SERVERS = 'localhost:19092' # Assuming Kafka is on localhost for this example
KAFKA_TOPIC = "names_topic"

# In-memory storage for dashboard data
dashboard_data = {
    "total_users": 0,
    "gender_distribution": {"male": 0, "female": 0},
    "users_by_nation": {},
    "users_by_city": {},
    "recent_users": []
}

MAX_RECENT_USERS = 10

def consume_from_kafka():
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'dashboard_consumer_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])

    print("Kafka Consumer started...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0) # Poll for messages with a timeout

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event - not an error
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break

            # Process the message
            data = json.loads(msg.value().decode('utf-8'))
            print(f"Consumed: {data['name']}, {data['nation']}")

            # Update dashboard data in-memory
            dashboard_data["total_users"] += 1
            
            gender = data.get("gender")
            if gender in dashboard_data["gender_distribution"]:
                dashboard_data["gender_distribution"][gender] += 1
            
            nation = data.get("nation")
            dashboard_data["users_by_nation"][nation] = dashboard_data["users_by_nation"].get(nation, 0) + 1
            
            city = data.get("city")
            dashboard_data["users_by_city"][city] = dashboard_data["users_by_city"].get(city, 0) + 1

            # Keep only the most recent users
            dashboard_data["recent_users"].insert(0, {
                "name": data["name"],
                "gender": data["gender"],
                "city": data["city"],
                "nation": data["nation"]
            })
            if len(dashboard_data["recent_users"]) > MAX_RECENT_USERS:
                dashboard_data["recent_users"].pop()

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("Kafka Consumer closed.")

def start_consumer_in_thread():
    consumer_thread = threading.Thread(target=consume_from_kafka)
    consumer_thread.daemon = True # Allow the main program to exit even if thread is running
    consumer_thread.start()
    return consumer_thread

if __name__ == "__main__":
    start_consumer_in_thread()
    # Keep the main thread alive to allow the consumer thread to run
    print("Main script running. Data is being consumed in the background.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Exiting main script.")