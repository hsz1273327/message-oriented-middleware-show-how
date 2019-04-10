import sys
import time
import random
import threading
from kafka import KafkaProducer
from kafka import KafkaConsumer


kfkproducer = KafkaProducer(bootstrap_servers='localhost:9092')

sourceQ = "sourceQ"
resultQ = "resultQ"
exitCh = "exitCh"


def push(Q, value):
    future = kfkproducer.send(Q,str(value).encode())
    result = future.get(timeout=10)
    print(f"send {value} to {Q},{result}")


def producer():
    while True:
        data = random.randint(1, 400)
        push(sourceQ, data)
        time.sleep(1)


def collector():
    sum = 0
    consumer = KafkaConsumer(resultQ, group_id=resultQ, bootstrap_servers='localhost:9092')
    for msg in consumer:
        print(f"received {msg}")
        sum += int(msg.value.decode("utf-8"))
        print(f"get sum {sum}")


def main():
    t = threading.Thread(target=collector, daemon=True)
    t.start()
    try:
        print("start")
        producer()
    except KeyboardInterrupt:
        push(exitCh, "Exit")
    except Exception as e:
        print(e)
        raise e
    finally:
        sys.exit()


if __name__ == "__main__":
    main()
