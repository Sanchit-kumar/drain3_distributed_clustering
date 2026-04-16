import re

def denoise_text(text: str) -> str:
    """Removes noise like hex and timestamps to improve Drain3 clustering."""
    text = str(text)
    text = re.sub(r'0x[a-fA-F0-9]+', '<HEX>', text)
    text = re.sub(r'\d{4}-\d{2}-\d{2}|\d{2}:\d{2}:\d{2}', '<TIME>', text)
    text = re.sub(r'\b\d+\b', '<NUM>', text)
    return " ".join(text.split())