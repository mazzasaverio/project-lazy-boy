import logging

from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

logger = logging.getLogger(__name__)
model = AutoModelForSeq2SeqLM.from_pretrained("google/flan-t5-small", cache_dir="./data")
tokenizer = AutoTokenizer.from_pretrained("google/flan-t5-small", cache_dir="./data")


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
