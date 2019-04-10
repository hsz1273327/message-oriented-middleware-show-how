import sys
import uuid
import time
import threading
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka import TopicPartition


kfkproducer = KafkaProducer(bootstrap_servers='localhost:9092')

sourceQ = "sourceQ"
resultQ = "resultQ"
exitCh = "exitCh"


def push(Q, value):
    future = kfkproducer.send(Q, str(value).encode())
    result = future.get(timeout=10)
    print(f"send {value} to {Q},{result}")


def get_result():
    PARTITION_0 = 0
    consumer = KafkaConsumer(sourceQ, group_id=sourceQ, bootstrap_servers='localhost:9092')
    for msg in consumer:
        print(f"received {msg}")
        data = int(msg.value.decode("utf-8"))
        push(resultQ, int(data)**2)


def main():
    t = threading.Thread(target=get_result, daemon=True)
    t.start()
    group_id = str(uuid.uuid4())
    print(f"group_id:{group_id}")
    consumer = KafkaConsumer(exitCh, group_id=group_id, bootstrap_servers='localhost:9092')
    for msg in consumer:
        print(msg)
        if msg.value == b"Exit":
            sys.exit(0)


if __name__ == "__main__":
    main()
