import asyncio
import json
import os.path
from urllib.parse import urlparse, urljoin

import httpx
from bs4 import BeautifulSoup
from redis.asyncio import Redis


async def main():
    r = await Redis(host='redis', port=6379, decode_responses=True)
    urls = []
    # await r.lpop(100000000)
    if os.path.exists("data/urls.json"):
        with open("data/urls.json", "r") as f:
            urls = json.load(f)
        os.rename("data/urls.json", "data/old_urls.json")
    if urls:
        await r.lpush("frontier", *urls)

    while True:
        url = await r.lpop("frontier")
        print(url)
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(url)
        except Exception as e:
            print(e)
            await r.rpush("frontier", url)
            continue
        await r.lpush("visited", url)
        soup = BeautifulSoup(resp.text, 'html.parser')

        for link in soup.find_all('a'):
            new_url = link.get('href')
            if not urlparse(new_url).netloc:
                new_url = urljoin(url, new_url)
            result = urlparse(new_url)
            if not all([result.scheme, result.netloc]):
                continue

            out = await r.lpos("visited", new_url)
            if out is None:
                print(new_url)
                await r.rpush("frontier", new_url)


if __name__ == "__main__":
    asyncio.run(main())
