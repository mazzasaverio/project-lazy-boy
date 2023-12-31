import asyncio
import itertools
import json
import logging.config
import os.path
import sys
import time

import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from httpx import Limits, Timeout
from redis.asyncio import Redis

load_dotenv()
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/crawler.log"),
        logging.StreamHandler(stream=sys.stdout),
    ],
)
logger = logging.getLogger(__name__)
URLS = "config/urls.json"


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

            soup = BeautifulSoup(resp.text, "html.parser")
            await r.lpush(
                "pages",
                *list(
                    set(
                        url + "--!!--" + s.get("href") for s in soup.find_all(href=True)
                    )
                ),
            )
            resps.append(1)
        except asyncio.exceptions.CancelledError as e:
            await r.rpush("frontier", url)
            resps.append(2)
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
    return resps


async def crawler():
    r = await Redis(host=os.getenv("REDIS_HOST"), port=6379, decode_responses=True)
    urls = []

    if os.path.exists(URLS):
        with open(URLS, "r") as f:
            urls = json.load(f)
        os.rename(URLS, "config/old_urls.json")
    if urls:
        await r.lpush("frontier", *urls)
    reqs = 500
    timeout = 10
    while True:
        async with httpx.AsyncClient(
            follow_redirects=True,
            http2=True,
            timeout=Timeout(timeout=timeout),
            limits=Limits(max_connections=reqs, max_keepalive_connections=20),
        ) as client:
            start = time.time()
            results = await asyncio.gather(
                *[crawl(client, r, ix) for ix in range(reqs)]
            )
            end = time.time()
            results = list(itertools.chain.from_iterable(results))
            succ = sum([1 for r in results if r == 1])
            errs = sum([1 for r in results if r == 0])
            timo = sum([1 for r in results if r == 2])
            tot = len(results)
            logger.warning(
                f"END: {end - start}, TOTAL: {tot}, SUCCESS: {succ}, TIMEOUTS: {timo}, ERROR: {errs}, TIMEOUT: {timeout}, REQUESTS: {reqs}"
            )
            if timo / max([1, tot]) > 0.1:
                timeout = min([timeout + 2, 40])
                reqs = max([reqs - 10, 200])
            elif timo / max([1, tot]) < 0.05:
                timeout = max([timeout - 2, 5])
                reqs = min([reqs + 10, 1000])


if __name__ == "__main__":
    try:
        asyncio.run(crawler())
    except Exception as ex:
        logger.error(ex)
