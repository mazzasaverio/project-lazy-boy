import logging

from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

logger = logging.getLogger(__name__)
model = AutoModelForSeq2SeqLM.from_pretrained("google/flan-t5-small")
tokenizer = AutoTokenizer.from_pretrained("google/flan-t5-small")


async def local_llm(url):
    prompt_template = f"""
    Classify career page urls:
    
    1) https://www.1mg.com/jobs: 1
    2) 3i-infotech.com/careers/: 1
    3) https://www.inquirer.com/opinion/commentary/saving-liberal-arts-education-america-20230604.html: 0
    4) https://www.247.ai/career-search: 1
    5) https://www.accenture.com/in-en/careers: 1
    6) https://www.nytimes.com/2021/10/19/business/work-spaces-design-employees.html: 0
    7) https://accord-global.com/careers.html: 1
    8) http://equinoxfarmberkshires.com/contact-us/: 0
    9) https://careers.alibaba.com: 1
    
    10) {url}:
    """

    inputs = tokenizer(prompt_template, return_tensors="pt")
    outputs = await model.agenerate(**inputs)
    return tokenizer.batch_decode(outputs, skip_special_tokens=True)
