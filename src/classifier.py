import logging
import os
from typing import List

from langchain.chat_models import AzureChatOpenAI
from langchain.schema import BaseMessage
from openai import InvalidRequestError

logger = logging.getLogger(__name__)


async def azure_openai_chat(
        messages: List[BaseMessage], temperature: float = 0.0
) -> str:
    llm = AzureChatOpenAI(
        openai_api_type=os.getenv("AZURE_OPENAI_API_TYPE"),
        openai_api_base=os.getenv("AZURE_OPENAI_API_BASE"),
        openai_api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
        openai_api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        deployment_name=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
        streaming=False,
        temperature=temperature,
    )
    try:
        result = await llm._call_async(messages)
        text = result.content
    except InvalidRequestError as ex:
        text = messages[-1].content
        logger.error(ex)
    except Exception as ex:
        text = messages[-1].content
        logger.error(ex)
    return text
