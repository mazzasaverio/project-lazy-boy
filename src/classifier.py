import logging
import os

import torch
from dotenv import load_dotenv
from transformers import AutoConfig, AutoTokenizer, AutoModelForSequenceClassification

logger = logging.getLogger(__name__)

load_dotenv()

MODEL_ID = os.getenv("MODEL_ID")
HF_CACHE = "../data"


def load_model():
    model_config = AutoConfig.from_pretrained(
        MODEL_ID, use_auth_token=os.getenv("HF_TOKEN"), cache_dir=HF_CACHE
    )

    model = AutoModelForSequenceClassification.from_pretrained(
        MODEL_ID,
        config=model_config,
        cache_dir=HF_CACHE,
        use_safetensors=False,
        low_cpu_mem_usage=True,
    )

    tokenizer = AutoTokenizer.from_pretrained(
        MODEL_ID,
        cache_dir=HF_CACHE,
    )
    return model, tokenizer


def local_llm(urls):
    model.eval()
    inputs = tokenizer.batch_encode_plus(
        urls, return_tensors="pt", padding=True, truncation=True
    )
    with torch.no_grad():
        outputs = model(**inputs)
    return outputs[0].argmax(dim=1).tolist()


model, tokenizer = load_model()

if __name__ == "__main__":
    print(local_llm(["http://rsga4u.com/apply.html", "http://rsga4u.com"]))
