import json
import os
from functools import lru_cache

from anthropic import Anthropic
from anthropic.types import ContentBlock
from dotenv import load_dotenv

from utils import disk_cached, throttle

MODEL_NAME = "claude-3-haiku-20240307"


@lru_cache
def get_client():
    load_dotenv()

    return Anthropic(
        # This is the default and can be omitted
        api_key=os.environ.get("ANTHROPIC_API_KEY"),
    )


@disk_cached(version=1)
@throttle(pause=12)
def complete_prompt(data: dict, prompt_template: str, gaslight: str, model: str = MODEL_NAME) -> str:
    print("API call", json.dumps(data)[:100], "...")
    client = get_client()
    prompt = prompt_template.format(**data)

    message = client.messages.create(
        max_tokens=128,
        messages=[
            {
                "role": "user",
                "content": prompt,
            },
            {
                "role": "assistant",
                "content": gaslight,
            },
        ],
        model=model,
    )
    msg: ContentBlock = message.content[0]
    return msg.text


if __name__ == '__main__':
    prompt_template = """\
    You will be writing a short, universal definition of a term found in provided context. The goal is
    to create a definition that could be used to add the term to Wikidata for future use.

    You will be provided with the following inputs:
    {name} - The name of the term you need to define.
    {category} - The category or domain the term belongs to.
    {text} - A short paragraph of context about the term.

    First, review the provided inputs carefully. Think about how you can use the name, category, and
    context information to write a clear, concise definition of the term.

    Next, write a short, universal definition of the term, approximately 10 words long. The
    definition should be general enough to be useful for adding the term to Wikidata, but specific
    enough to accurately capture the meaning of the term. Do not start the definition with the term name.

    Only output your definition inside <definition> tags."""

    gaslight = "<definition>"

    data = {
        "name": "nafo",
        "category": "group",
        "text": "fascinating aspect of ukraine's fight back against russian disinformation welcome to the world of nafo the fellows and images of shiba inu dogs confused listen on this hideous and barbaric"
    }
    data = {
        "name": "absolute territory",
        "category": "culture",
        "text": "so in Japan for example the very interest minute very interested in something called the absolute territory which is a strip of thigh here below a skirt and above the thigh high"
    }

    msg = complete_prompt(data, prompt_template, gaslight)
    print(msg)
