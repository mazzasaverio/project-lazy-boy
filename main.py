import asyncio
import json
import logging.config
import os.path

import httpx
from dotenv import load_dotenv
from redis.asyncio import Redis

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
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
        logger.info(f"Response from {ix} for {url}: Done")
    except Exception as e:
        await r.lpush("errors", url)
        logger.error(f"Response from {ix} for {url}: {e} {type(e)}")


async def crawler():
    r = await Redis(host=os.getenv("REDIS_HOST"), port=6379, decode_responses=True)
    urls = []

    if os.path.exists(URLS):
        with open(URLS, "r") as f:
            urls = json.load(f)
        os.rename(URLS, "data/old_urls.json")
    if urls:
        await r.lpush("frontier", *urls)

    async with httpx.AsyncClient(follow_redirects=True) as client:
        while True:
            results = await asyncio.gather(*[crawl(client, r, ix) for ix in range(100)])


if __name__ == "__main__":
    asyncio.run(crawler())
