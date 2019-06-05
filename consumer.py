import sys
import asyncio
import threading
from aredis import StrictRedis

REDIS_URL = "redis://localhost"
redis = StrictRedis.from_url(REDIS_URL)
p = redis.pubsub()
sourceQ = "sourceQ"
resultQ = "resultQ"
exitCh = "exitCh"


async def push(Q, value):
    if redis.exists(Q):
        if redis.type(Q) == "list":
            await redis.xlpush(Q, value)
        else:
            await redis.delete(Q)
            await redis.lpush(Q, value)
    else:
        await redis.lpush(Q, value)
    print(f"send {value} to {Q}")


async def get_result():
    while True:
        if await redis.exists(sourceQ) and await redis.type(sourceQ) == b"list":
            data = await redis.rpop(sourceQ)
            if data:
                print(f"received {data.decode()}")
                await push(resultQ, int(data)**2)
            else:
                await asyncio.sleep(1)
        else:
            await asyncio.sleep(1)


async def main():
    task = asyncio.ensure_future(get_result())
    await p.subscribe('exitCh')
    while True:
        message = await p.get_message()
        if message:
            if message.get("data") == b"Exit":
                p.close()
                sys.exit(0)
            else:
                await asyncio.sleep(1)
        else:
            await asyncio.sleep(1)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
