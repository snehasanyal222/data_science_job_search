import re
from typing import Optional

try:
    import spacy
    _nlp = spacy.load("en_core_web_sm")
except Exception:
    _nlp = None


def normalize_text(value: str) -> str:
    """Clean a text string for easier comparison and storage."""
    return " ".join(value.strip().split()) if value else ""


def parse_experience(value: str) -> str:
    """Use NLP patterns to normalize experience from text."""
    text = normalize_text(value).lower()
    if not text:
        return ""

    if _nlp:
        doc = _nlp(text)
        for ent in doc.ents:
            ent_text = ent.text.lower()
            if any(keyword in ent_text for keyword in ["yr", "year", "years"]):
                return ent.text

    patterns = [
        r"\b\d+\+?\s*(?:years?|yrs?)\b",
        r"\b\d+[-–]\d+\s*(?:years?|yrs?)\b",
        r"\b\d+\s*(?:years?|yrs?)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(0)

    return text


def parse_salary(value: str) -> str:
    """Use NLP and regex to extract salary amounts from text."""
    text = normalize_text(value).lower()
    if not text:
        return ""

    if _nlp:
        doc = _nlp(text)
        for ent in doc.ents:
            if ent.label_ == "MONEY":
                return ent.text

    patterns = [
        r"₹\s*\d+[.,]?\d*\s*(?:lpa|lakhs?|lac)?",
        r"\binr\s*\d+[.,]?\d*\b",
        r"\b\d+[.,]?\d*\s*(?:lpa|lakhs?|lac|per annum|pa|pm)\b",
        r"\bctc\s*[:–-]?\s*\d+[.,]?\d*\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(0)

    return text
