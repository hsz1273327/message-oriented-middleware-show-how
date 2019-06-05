import sys
import asyncio
import random
from aredis import StrictRedis

REDIS_URL = "redis://localhost"
redis = StrictRedis.from_url(REDIS_URL)


sourceQ = "sourceQ"
resultQ = "resultQ"
exitCh = "exitCh"


async def push(Q, value):
    try:
        if await redis.exists(Q):
            if await redis.type(Q) == "list":
                await redis.xlpush(Q, value)
            else:
                await redis.delete(Q)
                await redis.lpush(Q, value)
        else:
            await redis.lpush(Q, value)
        print(f"send {value} to {Q}")
    except KeyboardInterrupt:
        await redis.publish(exitCh, 'Exit')
    except Exception as e:
        raise e


async def producer():
    try:
        while True:
            data = random.randint(1, 400)
            await push(sourceQ, data)
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        await redis.publish(exitCh, 'Exit')
    except Exception as e:
        raise e


async def collector():
    try:
        sum = 0
        while True:
            if await redis.exists(resultQ) and await redis.type(resultQ) == b"list":
                data = await redis.rpop(resultQ)
                if data:
                    print(f"received {data.decode()}")
                    sum += int(data)
                    print(f"get sum {sum}")
                else:
                    await asyncio.sleep(1)
            else:
                await asyncio.sleep(1)
    except KeyboardInterrupt:
        await redis.publish(exitCh, 'Exit')
    except Exception as e:
        raise e


async def main():
    task = asyncio.ensure_future(collector())
    await producer()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        loop.run_until_complete(redis.publish(exitCh, 'Exit'))
    except Exception as e:
        raise e
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
