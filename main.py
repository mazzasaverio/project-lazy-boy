import asyncio
import json
import os.path
from urllib.parse import urlparse, urljoin

import httpx
import yaml
from bs4 import BeautifulSoup
from redis.asyncio import Redis


async def fetch(r, career_keywords, ix):
    i = 0
    while True:
        i += 1
        url = await r.lpop("frontier")
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(url)
            await r.lpush("visited", url)
            soup = BeautifulSoup(resp.text, 'html.parser')

            for link in soup.find_all('a'):
                new_url = link.get('href')
                if not urlparse(new_url).netloc:
                    new_url = urljoin(url, new_url)
                result = urlparse(new_url)
                if not all([result.scheme, result.netloc]):
                    continue
                car_urls = await r.lpos("career_urls", new_url)
                visited = await r.lpos("visited", new_url)
                if visited is not None or car_urls is not None:
                    continue
                if any([car in new_url for car in career_keywords]):
                    await r.rpush("career_urls", new_url)
                    await r.lpush("visited", new_url)
                    continue

                if visited is None:
                    await r.rpush("frontier", new_url)
        except Exception as e:
            await r.rpush("frontier", url)

        print(f"Response {i} from {ix} for {url}: Done")


async def main():
    with open("data/config.yml", "r") as f:
        config = yaml.load(f, yaml.Loader)
        career_keywords = config.get("career_keywords")

    r = await Redis(host='redis', port=6379, decode_responses=True)
    urls = []
    # await r.delete("frontier")
    # await r.delete("visited")

    URLS = "data/urls.json"
    if os.path.exists(URLS):
        with open(URLS, "r") as f:
            urls = json.load(f)
        os.rename(URLS, "data/old_urls.json")
    if urls:
        await r.lpush("frontier", *urls)

    tasks = [fetch(r, career_keywords, ix) for ix in range(100)]
    responses = await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
