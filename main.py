import asyncio
import json
import logging.config
import os.path
import re
from urllib.parse import urlparse, urljoin, urlunparse

import httpx
import yaml
from bs4 import BeautifulSoup
from redis.asyncio import Redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("data/app.log")
    ]
)
logger = logging.getLogger(__name__)
URLS = "data/urls.json"


async def scrape(resp, url: str, r, career_keywords: list):
    soup = BeautifulSoup(resp.text, 'html.parser')

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


async def crawl(r, career_keywords: list, ix: int):
    i = 0
    while True:
        i += 1
        url = await r.lpop("frontier")
        try:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                resp = await client.get(url)
                resp.raise_for_status()
            await r.lpush("visited", url)
            await scrape(resp, url, r, career_keywords)
        except Exception as e:
            await r.lpush("errors", url)
            logger.error(f"Response {i} from {ix} for {url}: {e}")
            continue
        logger.info(f"Response {i} from {ix} for {url}: Done")


async def crawler():
    with open("data/config.yml", "r") as f:
        config = yaml.load(f, yaml.Loader)
        career_keywords = config.get("career_keywords")

    r = await Redis(host='redis', port=6379, decode_responses=True)
    urls = []

    if os.path.exists(URLS):
        with open(URLS, "r") as f:
            urls = json.load(f)
        os.rename(URLS, "data/old_urls.json")
    if urls:
        await r.lpush("frontier", *urls)

    tasks = [crawl(r, career_keywords, ix) for ix in range(100)]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(crawler())
