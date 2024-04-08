def format_str_postal_codes_wikipedia(text: str) -> str:
    text = text.replace("–", "-")
    text = text.replace("−", "-")
    return text