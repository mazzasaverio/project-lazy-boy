import asyncio
import logging.config
import os
import re
import sys
import time
from urllib.parse import urlparse, urljoin, urlunparse

import yaml
from dotenv import load_dotenv
from redis.asyncio import Redis

from src.classifier import local_llm

load_dotenv()
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("data/scraping.log"),
        logging.StreamHandler(stream=sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

domain_pattern = r".*\.(com|co\.uk|org|net|gov|edu|it|io|tech|ai|app|dev)/.*"


def partition(pred, iterable):
    trues = []
    falses = []
    for item in iterable:
        if pred(item):
            trues.append(item)
        else:
            falses.append(item)
    return trues, falses


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
    pages = await r.lpop("pages", 10000)
    urls = [p.split("--!!--", 1) for p in pages]
    urls = [urljoin(url, new_url) if not urlparse(new_url).netloc else new_url for url, new_url in urls]
    urls = [dns_translation(u) for u in urls if all([urlparse(u).scheme, urlparse(u).netloc])]
    urls = [u for u in urls if not any(re.search(pattern, u) for pattern in exclude_patterns)]
    urls = [u for u in urls if not any([dom in u for dom in social_network_domains])]
    urls = list(set(u for u in urls if re.match(domain_pattern, u)))
    for clean_url in urls:
        car_urls = await r.lpos("career_urls", clean_url)
        visited = await r.lpos("visited", clean_url)
        if visited is not None or car_urls is not None:
            urls.remove(clean_url)
    possible_career_urls, new_frontier = partition(lambda x: any([car in x for car in career_keywords]), urls)
    llm_judge = local_llm(possible_career_urls)
    new_career_urls = []
    for is_career_url, clean_url in zip(llm_judge, possible_career_urls):
        try:
            if int(is_career_url):
                new_career_urls.append(clean_url)
            else:
                new_frontier.append(clean_url)
        except Exception as ex:
            new_frontier.append(clean_url)
    await r.rpush("visited", *new_career_urls)
    await r.rpush("career_urls", *new_career_urls)
    await r.rpush("frontier", *new_frontier)

    return len(pages), len(new_career_urls)


async def scraper():
    with open("data/config.yml", "r") as f:
        config = yaml.load(f, yaml.Loader)
        career_keywords = config.get("career_keywords")
        exclude_patterns = config.get("exclude_patterns")
        social_network_domains = config.get("social_network_domains")

    r = await Redis(host=os.getenv("REDIS_HOST"), port=6379, decode_responses=True)
    while True:
        start = time.time()
        tot = 0
        tot_car = 0
        while time.time() - start < 60:
            try:
                filtered, new_career_urls = await scrape(r, career_keywords, exclude_patterns, social_network_domains)
                tot += filtered
                tot_car += new_career_urls
            except Exception as e:
                time.sleep(30)
        logger.warning(
            f"END: {time.time() - start}, TOTAL: {tot}, NEW CAREER URLS: {tot_car}")


if __name__ == "__main__":
    asyncio.run(scraper())
