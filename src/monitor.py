import asyncio
import logging.config
import os
import sys

from dotenv import load_dotenv
from redis.asyncio import Redis

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/monitor.log"),
        logging.StreamHandler(stream=sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


async def monitor():
    try:
        r = await Redis(host=os.getenv("REDIS_HOST"), port=6379, decode_responses=True)

        prev = 0
        prev_car = 0
        while True:
            await asyncio.sleep(60)
            result = await r.llen("visited")
            logger.info(f"VISITED URLS: {result - prev}")
            prev = result
            result = await r.llen("career_urls")
            logger.info(f"NEW CAREER URLS: {result - prev_car}")
            prev_car = result
    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    asyncio.run(monitor())
