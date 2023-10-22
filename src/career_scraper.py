import asyncio
import logging
import os
import sys
import time

import httpx
import trafilatura
from dotenv import load_dotenv
from httpx import Limits, Timeout
from langchain.chat_models import AzureChatOpenAI
from langchain.schema import SystemMessage, HumanMessage
from redis.asyncio import Redis

load_dotenv()
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/career_scraping.log"),
        logging.StreamHandler(stream=sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

llm = AzureChatOpenAI(
    openai_api_type=os.getenv("AZURE_OPENAI_API_TYPE"),
    openai_api_base=os.getenv("AZURE_OPENAI_API_BASE"),
    openai_api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
    openai_api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    deployment_name=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
    streaming=False,
    temperature=0.0,
)


async def scrape(r):
    urls = await r.lpop("career_urls", 10)
    if urls is None:
        return 0
    try:
        async with httpx.AsyncClient(
                follow_redirects=True,
                http2=True,
                timeout=Timeout(timeout=10),
                limits=Limits(max_connections=500, max_keepalive_connections=20),
        ) as client:
            for url in urls:
                try:

                    resp = await client.get(url)
                    resp.raise_for_status()
                except Exception as ex:
                    logger.error(ex)
                    await r.rpush("career_urls", url)
                    continue
                result = trafilatura.extract(resp.text)
                messages = [
                    SystemMessage(
                        content="Please classify the provided text as 0 if it is a listing of job postings with a list of "
                                "different job openings with short descriptions of job search, "
                                "1 if it is a job description for a specific job opening that includes information"
                                "like responsibilities, qualifications, and application instructions), "
                                "2 otherwise. Enter your response as a valid Python integer."
                    ),
                    HumanMessage(
                        content=result[:1000]
                    )
                ]
                try:
                    llm_output = llm(messages)
                except Exception as e:
                    logger.error(e)
                    continue
                label = int(llm_output.content)
                if label == 1:
                    await r.lpush("job_page", url)
                elif label == 0:
                    await r.lpush("job_posting", url)
                elif label == 2:
                    await r.lpush("maybe_job_page", url)
                else:
                    await r.rpush("career_urls", url)
    except Exception as ex:
        logger.error(ex)
    return len(urls)


async def scraper():
    r = await Redis(host=os.getenv("REDIS_HOST"), port=6379, decode_responses=True)
    while True:
        start = time.time()
        tot = 0
        while time.time() - start < 60:
            try:
                filtered = await scrape(r)
                tot += filtered
            except Exception as e:
                logger.error(e)
        logger.warning(
            f"END: {time.time() - start}, TOTAL: {tot}"
        )


if __name__ == "__main__":
    try:
        asyncio.run(scraper())
    except Exception as ex:
        logger.error(ex)
