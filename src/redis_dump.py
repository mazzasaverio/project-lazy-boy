import asyncio
import json

from redis.asyncio import Redis


async def main():
    r = await Redis(host="localhost", port=6379, decode_responses=True)
    career_urls = []
    frontier = []

    try:
        while True:
            pages = await r.lpop("career_urls", 5000)
            career_urls.extend(pages)
    except Exception as ex:
        pass

    try:
        while len(frontier) < 200000:
            pages = await r.rpop("frontier", 5000)
            frontier.extend(pages)
    except Exception as ex:
        pass
    with open("dataset2.json", "w") as f:
        json.dump({"career_urls": career_urls, "frontier": frontier}, f)


if __name__ == "__main__":
    asyncio.run(main())
