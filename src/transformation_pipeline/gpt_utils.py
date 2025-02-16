from openai import OpenAI
import openai
import time




# process_row function remains in gpt_utils.py (or similar)
def process_row(
        openai_api_key: str,
        product_description: str,
):
    """
    Calls the OpenAI API (ChatGPT) to generate category tags based on the provided product description.

    Uses exponential backoff to handle rate limiting. Returns the LLM-generated text.

    :param openai_api_key: API key for OpenAI.
    :param product_description: The product description text to analyze.
    :return: The response text from ChatGPT.
    """
    prompt = f"""
    Please read this product description: {product_description}
    Generate a list of category tags that capture the main themes, attributes, and use cases of the product. 
    The tags should be concise and descriptive. For example, if the description talks about a "wireless mouse" with 
    features like "ergonomic" and "compact", suitable tags might be "Computer Accessories", "Ergonomic", "Wireless".
    Please output the results in a string.
    """

    openai_client = openai.OpenAI(api_key=openai_api_key)
    max_retries = 5
    retry_delay = 5  # seconds

    for attempt in range(max_retries):
        try:
            response = openai_client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are an expert merchandiser."},
                    {"role": "user", "content": prompt}
                ]
            )
            return response.choices[0].message.content

        except openai.RateLimitError:
            wait_time = retry_delay * (2 ** attempt)
            print(f"Rate limit hit. Retrying in {wait_time:.2f} seconds... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(wait_time)

    print("Max retries reached. Skipping this request.")
    return "Error: Rate limit exceeded"
