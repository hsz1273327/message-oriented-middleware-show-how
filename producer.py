import sys
import time
import random
import threading
from redis import Redis

redis = Redis.from_url('redis://localhost:6379/2')

sourceQ = "sourceQ"
resultQ = "resultQ"
exitCh = "exitCh"


def push(Q, value):
    if redis.exists(Q):
        if redis.type(Q) == "list":
            redis.xlpush(Q, value)
        else:
            redis.delete(Q)
            redis.lpush(Q, value)
    else:
        redis.lpush(Q, value)
    print(f"send {value} to {Q}")


def producer():
    while True:
        data = random.randint(1, 400)
        push(sourceQ, data)
        time.sleep(1)


def collector():
    sum = 0
    while True:
        if redis.exists(resultQ) and redis.type(resultQ) == b"list":
            data = redis.rpop(resultQ)
            if data:
                print(f"received {data.decode()}")
                sum += int(data)
                print(f"get sum {sum}")
            else:
                time.sleep(1)
        else:
            time.sleep(1)


def main():
    t = threading.Thread(target=collector, daemon=True)
    t.start()
    try:
        producer()
    except KeyboardInterrupt:
        redis.publish(exitCh, 'Exit')
    except Exception as e:
        raise e
    finally:
        sys.exit()


if __name__ == "__main__":
    main()
