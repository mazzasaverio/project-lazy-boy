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


async def crawl(r, ix: int):
    i = 0
    while True:
        i += 1
        url = await r.lpop("frontier")
        try:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                resp = await client.get(url)
                resp.raise_for_status()
            await r.lpush("visited", url)
            await r.lpush("pages", f"{url}--!!--{resp.text}")
        except Exception as e:
            await r.lpush("errors", url)
            logger.error(f"Response {i} from {ix} for {url}: {e} {type(e)}")
            continue
        logger.info(f"Response {i} from {ix} for {url}: Done")


async def crawler():
    r = await Redis(host=os.getenv("REDIS_HOST"), port=6379, decode_responses=True)
    urls = []

    if os.path.exists(URLS):
        with open(URLS, "r") as f:
            urls = json.load(f)
        os.rename(URLS, "data/old_urls.json")
    if urls:
        await r.lpush("frontier", *urls)

    tasks = [crawl(r, ix) for ix in range(100)]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(crawler())
