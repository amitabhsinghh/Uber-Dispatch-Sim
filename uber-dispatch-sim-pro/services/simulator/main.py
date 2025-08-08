import os, json, time, random, string
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
RIDES_TOPIC = os.getenv("RIDES_TOPIC", "rides.v1")

def rand_id(prefix, n=8):
    return prefix + ''.join(random.choices(string.ascii_lowercase + string.digits, k=n))

p = Producer({'bootstrap.servers': KAFKA_BROKER})

def send(topic, value):
    p.produce(topic, json.dumps(value).encode("utf-8"))
    p.flush()

def main():
    # seed available drivers in Redis? We'll just emit driver IDs to a file;
    # the dispatch service pops from Redis. In a real setup, a driver service would register.
    print("simulator producing ride requests...")
    while True:
        ride = {
            "ride_id": rand_id("ride_"),
            "rider_id": rand_id("rider_"),
            "lat": round(random.uniform(12.8, 13.1), 6),
            "lng": round(random.uniform(77.5, 77.8), 6)
        }
        send(RIDES_TOPIC, ride)
        print("sent ride", ride["ride_id"])
        time.sleep(2)

if __name__ == "__main__":
    main()
