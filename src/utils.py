def normalize_text(value: str) -> str:
    """Clean a text string for easier comparison and storage."""
    return " ".join(value.strip().split()) if value else ""


def parse_experience(value: str) -> str:
    """Normalize experience text to a simple format."""
    if not value:
        return ""
    normalized = value.lower().replace("years", "yrs").replace("year", "yr")
    return normalized
