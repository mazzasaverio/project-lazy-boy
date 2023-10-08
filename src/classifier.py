import logging
import os

from torch import bfloat16
from transformers import AutoModelForSeq2SeqLM
from transformers import (
    BitsAndBytesConfig,
    AutoConfig,
    AutoTokenizer,
)

logger = logging.getLogger(__name__)
MODEL_ID = "google/flan-t5-base"
HF_CACHE = "./data"
bnb_config = BitsAndBytesConfig(
    load_in_4bit=True,
    bnb_4bit_quant_type="nf4",
    bnb_4bit_use_double_quant=True,
    bnb_4bit_compute_dtype=bfloat16,
)

model_config = AutoConfig.from_pretrained(
    MODEL_ID, use_auth_token=os.getenv("HF_TOKEN"), cache_dir=HF_CACHE
)

model = AutoModelForSeq2SeqLM.from_pretrained(
    MODEL_ID,
    config=model_config,
    quantization_config=bnb_config,
    device_map="auto",
    cache_dir=HF_CACHE,
)

tokenizer = AutoTokenizer.from_pretrained(
    MODEL_ID,
    cache_dir=HF_CACHE,
)

model.eval()


def local_llm(urls):
    raw_inputs = []
    for url in urls:
        prompt_template = f"""
        Classify 10 career page urls:
        
        1) https://www.1mg.com/jobs: 1
        2) http://iannonedesign.com/custom-woodwork: 0
        3) https://www.inquirer.com/opinion/commentary/saving-liberal-arts-education-america-20230604.html: 0
        4) https://www.accenture.com/in-en/careers: 1
        5) https://www.nytimes.com/2021/10/19/business/work-spaces-design-employees.html: 0
        6) https://accord-global.com/careers.html: 1
        7) http://equinoxfarmberkshires.com/contact-us/: 0
        8) http://rsga4u.com/apply.html: 1
        9) http://heywoodfinance.co.uk/intermediaries/working-with-us.php: 1
        10) {url}:
        """
        raw_inputs.append(prompt_template)

    inputs = tokenizer.batch_encode_plus(raw_inputs, return_tensors="pt", padding=True)
    outputs = model.generate(**inputs)
    return tokenizer.batch_decode(outputs, skip_special_tokens=True)


if __name__ == "__main__":
    print(local_llm("http://openinstall.com/join.html"))
