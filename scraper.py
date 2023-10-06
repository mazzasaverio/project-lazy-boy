import asyncio
import logging.config
import os
import re
import time
from urllib.parse import urlparse, urljoin, urlunparse

import yaml
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from redis.asyncio import Redis

from redis import BusyLoadingError

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("data/scraping.log")
    ]
)
logger = logging.getLogger(__name__)


def dns_translation(url: str):
    parsed_url = urlparse(url)

    clean_url = urlunparse((
        parsed_url.scheme,
        parsed_url.netloc,
        parsed_url.path,
        parsed_url.params,
        '',
        parsed_url.fragment
    ))

    clean_url = re.sub(r'/$', '', clean_url)
    return clean_url


async def scrape(r, career_keywords: list):
    while True:
        try:
            txt = await r.lpop("pages")
            url, txt = txt.split("--!!--", 1)
            soup = BeautifulSoup(txt, 'html.parser')
            for link in soup.find_all('a'):
                new_url = link.get('href')
                if not urlparse(new_url).netloc:
                    new_url = urljoin(url, new_url)
                result = urlparse(new_url)
                if not all([result.scheme, result.netloc]):
                    continue
                clean_url = dns_translation(new_url)
                car_urls = await r.lpos("career_urls", clean_url)
                visited = await r.lpos("visited", clean_url)
                if visited is not None or car_urls is not None:
                    continue
                if any([car in new_url for car in career_keywords]):
                    await r.rpush("career_urls", new_url)
                    await r.lpush("visited", new_url)
                    continue

                if visited is None:
                    await r.rpush("frontier", new_url)

        except Exception as e:
            logger.error(f"Extraction: {str(e)}, {type(e)}")


async def scraper():
    with open("data/config.yml", "r") as f:
        config = yaml.load(f, yaml.Loader)
        career_keywords = config.get("career_keywords")

    r = await Redis(host=os.getenv("REDIS_HOST"), port=6379, decode_responses=True)
    while True:
        try:
            r.ping()
            break
        except BusyLoadingError:
            time.sleep(1)
    tasks = [scrape(r, career_keywords) for _ in range(10)]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(scraper())
