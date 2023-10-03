import asyncio
import json
import os.path
from urllib.parse import urlparse, urljoin

import httpx
from bs4 import BeautifulSoup
from redis.asyncio import Redis


async def main():
    r = await Redis(host='localhost', port=6379, decode_responses=True)
    urls = []
    if os.path.exists("urls.json"):
        with open("urls.json", "r") as f:
            urls = json.load(f)
        os.rename("urls.json", "old_urls.json")
    await r.lpush("frontier", *urls)

    while True:
        url = await r.lpop("frontier")
        await r.lpush("visited", url)

        async with httpx.AsyncClient() as client:
            resp = await client.get(url)
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
                await r.lpush("frontier", new_url)


if __name__ == "__main__":
    asyncio.run(main())
