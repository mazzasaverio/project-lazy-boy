import asyncio
import itertools
import json
import logging.config
import os.path
import time

import httpx
from dotenv import load_dotenv
from httpx import Limits, Timeout
from redis.asyncio import Redis

from redis import BusyLoadingError

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
    start = time.time()
    resps = []
    times = []
    while time.time() - start < 20:
        url = await r.lpop("frontier")
        resp = None
        try:
            start_req = time.time()
            resp = await client.get(url)
            end_req = time.time()
            times.append(end_req - start_req)
            resp.raise_for_status()
            await r.lpush("visited", url)
            await r.lpush("pages", f"{url}--!!--{resp.text}")
            resps.append(1)
        except httpx.ConnectTimeout as e:
            await r.rpush("frontier", url)
            resps.append(2)
        except httpx.HTTPStatusError as e:
            if resp.status_code in [403, 404, 500, 999, 502]:
                resps.append(0)
                continue
            await r.lpush("errors", url)
            resps.append(0)
        except Exception as e:
            await r.lpush("errors", url)
            resps.append(0)
        finally:
            if resp is not None:
                await resp.aclose()
    print(times)
    return resps


async def crawler():
    r = await Redis(host=os.getenv("REDIS_HOST"), port=6379, decode_responses=True)
    urls = []

    while True:
        try:
            r.ping()
            break
        except BusyLoadingError:
            time.sleep(1)

    if os.path.exists(URLS):
        with open(URLS, "r") as f:
            urls = json.load(f)
        os.rename(URLS, "data/old_urls.json")
    if urls:
        await r.lpush("frontier", *urls)
    reqs = 500
    timeout = 10
    while True:
        async with httpx.AsyncClient(follow_redirects=True, http2=True,
                                     timeout=Timeout(timeout=timeout),
                                     limits=Limits(max_connections=reqs, max_keepalive_connections=20)) as client:
            start = time.time()
            results = await asyncio.gather(*[crawl(client, r, ix) for ix in range(reqs)])
            end = time.time()
            results = list(itertools.chain.from_iterable(results))
            succ = sum([1 for r in results if r == 1])
            errs = sum([1 for r in results if r == 0])
            timo = sum([1 for r in results if r == 2])
            tot = len(results)
            logger.warning(
                f"END: {end - start}, TOTAL: {tot}, SUCCESS: {succ}, TIMEOUTS: {timo}, ERROR: {errs}")
            if timo / tot > 0.1:
                timeout = min([timeout + 5, 40])
                reqs = min([timeout - 50, 200])
            elif timeout < 0.05:
                timeout = max([timeout - 5, 5])
                reqs = max([timeout + 50, 100])


if __name__ == "__main__":
    asyncio.run(crawler())
