import asyncio
import json
import os.path
from urllib.parse import urlparse, urljoin

import httpx
import yaml
from bs4 import BeautifulSoup
from redis.asyncio import Redis


async def fetch(r, career_keywords):
    url = await r.lpop("frontier")
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url)
    except Exception as e:
        await r.rpush("frontier", url)
        return url, str(e)
    await r.lpush("visited", url)
    soup = BeautifulSoup(resp.text, 'html.parser')

    for link in soup.find_all('a'):
        new_url = link.get('href')
        if not urlparse(new_url).netloc:
            new_url = urljoin(url, new_url)
        result = urlparse(new_url)
        if not all([result.scheme, result.netloc]):
            continue
        if any([car in new_url for car in career_keywords]):
            print(new_url)
            await r.rpush("career_urls", new_url)
            continue

        out = await r.lpos("visited", new_url)

        if out is None:
            await r.rpush("frontier", new_url)

    return url, "Done"


async def main():
    with open("data/config.yml", "r") as f:
        config = yaml.load(f, yaml.Loader)
        career_keywords = config.get("career_keywords")

    r = await Redis(host='localhost', port=6379, decode_responses=True)
    urls = []
    await r.lpop("frontier", 100000000)
    await r.lpop("visited", 100000000)

    URLS = "data/temp.json"
    if os.path.exists(URLS):
        with open(URLS, "r") as f:
            urls = json.load(f)
        os.rename(URLS, "data/old_urls.json")
    if urls:
        await r.lpush("frontier", *urls)
    i = 0
    while True:
        tasks = [fetch(r, career_keywords) for _ in range(100)]
        responses = await asyncio.gather(*tasks)
        for url, response in responses:
            i += 1
            print(f"Response {i} from {url}: {response}")


if __name__ == "__main__":
    asyncio.run(main())
