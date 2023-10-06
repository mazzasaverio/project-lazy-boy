import asyncio
import logging.config
import os
import re
from urllib.parse import urlparse, urljoin, urlunparse

import yaml
from dotenv import load_dotenv
from redis.asyncio import Redis

from src.classifier import local_llm

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("data/scraping.log")
    ]
)
logger = logging.getLogger(__name__)

domain_pattern = r".*\.(com|co\.uk|org|net|gov|edu|it|io|tech|ai|app|dev)/.*"


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


async def scrape(r, career_keywords: list, exclude_patterns, social_network_domains):
    while True:
        try:
            txt = await r.lpop("pages")
            url, new_url = txt.split("--!!--", 1)
            if not urlparse(new_url).netloc:
                new_url = urljoin(url, new_url)
            result = urlparse(new_url)
            if not all([result.scheme, result.netloc]):
                continue
            clean_url = dns_translation(new_url)
            if any(re.search(pattern, new_url) for pattern in exclude_patterns):
                continue
            if any([dom in new_url for dom in social_network_domains]):
                continue

            if not re.match(domain_pattern, new_url):
                continue

            car_urls = await r.lpos("career_urls", clean_url)
            visited = await r.lpos("visited", clean_url)
            if visited is not None or car_urls is not None:
                continue
            if any([car in new_url for car in career_keywords]):
                is_career_url = await local_llm(new_url)
                if int(is_career_url):
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
        exclude_patterns = config.get("exclude_patterns")
        social_network_domains = config.get("social_network_domains")

    r = await Redis(host=os.getenv("REDIS_HOST"), port=6379, decode_responses=True)

    tasks = [scrape(r, career_keywords, exclude_patterns, social_network_domains) for _ in range(10)]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(scraper())
