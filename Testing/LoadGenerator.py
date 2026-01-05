import argparse
import json
import random
import time
from confluent_kafka import Producer

def send(producer, topic, obj):
    producer.produce(topic, value=json.dumps(obj).encode("utf-8"))
    producer.poll(0)
    
    


def main():
    
    
#---- the main arguments that can be passed when the program runs to adjust the load----

    p = argparse.ArgumentParser("Simple Kafka loadgen (users + cart)")
    p.add_argument("--bootstrap", default="localhost:29092")
    p.add_argument("--topic-users", default="user-events")
    p.add_argument("--topic-cart", default="cart-events")
    p.add_argument("--rate", type=float, default=500000.0, help="events per second")
    p.add_argument("--duration", type=float, default=10.0, help="seconds")
    p.add_argument("--users", type=int, default=50, help="userId range: 1..users")
    p.add_argument("--mode", choices=["register", "delete", "cart"])   #  , required=True)
    args = p.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap})

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    end = time.time() + args.duration
    sent = 0

    try:
        while time.time() < end:
            uid = random.randint(1, args.users)

            if args.mode == "register":
                evt = {"userId": uid, "userName": f"User{uid}", "eventType": "REGISTER"}
                send(producer, args.topic_users, evt)

            elif args.mode == "delete":
                evt = {"userId": uid, "eventType": "DELETE"}
                send(producer, args.topic_users, evt)

            else:  # cart
                # minimal cart event 
                evt = {
                    "userId": uid,
                    "userName": f"User{uid}",
                    "productId": random.randint(1, 10),
                    "delta": random.choice([1, 1, 1, -1]),  # mostly add
                    "action": random.choice(["ADD", "REMOVE"]),
                }
                send(producer, args.topic_cart, evt)

            sent += 1
            if interval > 0:
                time.sleep(interval)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(10)

    print(f"sent={sent}")

if __name__ == "__main__":
    main()