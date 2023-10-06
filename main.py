import asyncio
import json
import logging.config
import os.path
import time

import httpx
from dotenv import load_dotenv
from httpx import Limits, Timeout
from redis.asyncio import Redis

load_dotenv()
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("data/app.log")
    ]
)
logger = logging.getLogger(__name__)
URLS = "data/urls.json"


async def crawl(client, r, ix: int):
    url = await r.lpop("frontier")
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        await r.lpush("visited", url)
        await r.lpush("pages", f"{url}--!!--{resp.text}")
        return 1
    except httpx.ConnectTimeout as e:
        await r.rpush("frontier", url)
        return 2
    except httpx.HTTPStatusError as e:
        if resp.status_code in [403, 404, 500, 999, 502]:
            return 0
        await r.lpush("errors", url)
        return 0
    except Exception as e:
        await r.lpush("errors", url)
        return 0


async def crawler():
    r = await Redis(host=os.getenv("REDIS_HOST"), port=6379, decode_responses=True)
    urls = []

    if os.path.exists(URLS):
        with open(URLS, "r") as f:
            urls = json.load(f)
        os.rename(URLS, "data/old_urls.json")
    if urls:
        await r.lpush("frontier", *urls)
    reqs = 500
    while True:
        async with httpx.AsyncClient(follow_redirects=True, http2=True,
                                     timeout=Timeout(timeout=10.0),
                                     limits=Limits(max_connections=reqs, max_keepalive_connections=20)) as client:
            start = time.time()
            results = await asyncio.gather(*[crawl(client, r, ix) for ix in range(reqs)])
            end = time.time()
            logger.warning(
                f"END: {end - start}, SUCCESS: {sum([1 for r in results if r == 1])}, TIMEOUTS: {sum([1 for r in results if r == 2])}, ERROR: {sum([1 for r in results if r == 0])}")


if __name__ == "__main__":
    asyncio.run(crawler())
