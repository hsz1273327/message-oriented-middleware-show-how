import sys
import time
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


def get_result():
    while True:
        if redis.exists(sourceQ) and redis.type(sourceQ) == b"list":
            data = redis.rpop(sourceQ)
            if data:
                print(f"received {data.decode()}")
                push(resultQ, int(data)**2)
            else:
                time.sleep(1)
        else:
            time.sleep(1)


def main():
    t = threading.Thread(target=get_result, daemon=True)
    t.start()
    p = redis.pubsub()
    p.subscribe('exitCh')
    while True:
        message = p.get_message()
        if message:
            if message.get("data") == b"Exit":
                p.close()
                sys.exit(0)
            else:
                time.sleep(1)
        else:
            time.sleep(1)


if __name__ == "__main__":
    main()
