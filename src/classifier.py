import asyncio
import logging
import os

from langchain.chat_models import AzureChatOpenAI
from langchain.schema import HumanMessage, SystemMessage

logger = logging.getLogger(__name__)


async def azure_openai_chat(url) -> str:
    llm = AzureChatOpenAI(
        openai_api_type=os.getenv("AZURE_OPENAI_API_TYPE"),
        openai_api_base=os.getenv("AZURE_OPENAI_API_BASE"),
        openai_api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
        openai_api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        deployment_name=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME"),
        streaming=False,
        temperature=0.0,
    )
    result = await llm._call_async(
        [
            SystemMessage(
                content="""Generate an email in JSON format with the following criteria:
                    Subject: [Concise subject of the Email]
                    Main Content: [Provide the main message or content you want in the email]
                    Only use the information in the prompt, do not insert placeholders to replace/fill.
                    Do not add factually unverified information.
                    Please generate the email as a JSON object with two fields: 'subject' and 'content'."""
            ),
            HumanMessage(
                content=url,
            ),
        ]
    )

    return result.content


if __name__ == "__main__":
    asyncio.run(azure_openai_chat(""))
