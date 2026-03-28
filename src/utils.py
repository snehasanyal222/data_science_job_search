import re

try:
    import spacy
    _nlp = spacy.load("en_core_web_sm")
except Exception:
    _nlp = None


def normalize_text(value: str) -> str:
    """Clean a text string for easier comparison and storage."""
    return " ".join(value.strip().split()) if value else ""


# Ordered from most specific to least specific so the first match is the best.
_EXPERIENCE_PATTERNS = [
    # Range with + at top end: "5-8+ years"
    r"\b(\d+)\s*[-–]\s*(\d+)\+?\s*(?:years?|yrs?)\s*(?:of\s+(?:work\s+)?experience)?\b",
    # Minimum with +: "5+ years"
    r"\b(\d+)\+\s*(?:years?|yrs?)\s*(?:of\s+(?:work\s+)?experience)?\b",
    # Plain range: "3-5 years"
    r"\b(\d+)\s*[-–]\s*(\d+)\s*(?:years?|yrs?)\s*(?:of\s+(?:work\s+)?experience)?\b",
    # Single value: "3 years"
    r"\b(\d+)\s*(?:years?|yrs?)\s*(?:of\s+(?:work\s+)?experience)?\b",
    # Phrases like "minimum 3 years" / "at least 2 years"
    r"(?:minimum|at\s+least|atleast|min\.?)\s+(\d+)\s*(?:years?|yrs?)\b",
    # Bare abbreviated form: "2+ yrs" / "3 yrs"
    r"\b(\d+)\s*\+?\s*yrs?\b",
]


def parse_experience(value: str) -> str:
    """Extract the experience requirement from a text string.

    Returns a normalised string such as "3+ years" or "3-5 years",
    or "" if no recognisable experience requirement is found.
    """
    text = normalize_text(value)
    if not text:
        return ""

    # spaCy NER pass — catches free-form entities labelled with year keywords.
    if _nlp:
        doc = _nlp(text.lower())
        for ent in doc.ents:
            ent_text = ent.text.lower()
            if any(kw in ent_text for kw in ("yr", "year", "yrs")):
                raw = re.search(r"\d+[\+\-–]?\d*\s*(?:years?|yrs?)", ent_text)
                if raw:
                    return raw.group(0).strip()

    lower = text.lower()
    for pattern in _EXPERIENCE_PATTERNS:
        match = re.search(pattern, lower)
        if match:
            return match.group(0).strip()

    return ""


_SKILL_ALIASES = {
    "python": [r"\bpython\b"],
    "sql": [r"\bsql\b"],
    "r": [r"\br\b", r"\br language\b"],
    "tableau": [r"\btableau\b"],
    "power bi": [r"\bpower\s*bi\b"],
    "excel": [r"\bexcel\b"],
    "pandas": [r"\bpandas\b"],
    "numpy": [r"\bnumpy\b"],
    "scikit-learn": [r"\bscikit\s*[- ]?learn\b", r"\bsklearn\b"],
    "tensorflow": [r"\btensorflow\b"],
    "pytorch": [r"\bpytorch\b"],
    "spark": [r"\bspark\b", r"\bpyspark\b"],
    "aws": [r"\baws\b", r"\bamazon web services\b"],
    "azure": [r"\bazure\b"],
    "gcp": [r"\bgcp\b", r"\bgoogle cloud\b"],
    "machine learning": [r"\bmachine learning\b", r"\bml\b"],
    "deep learning": [r"\bdeep learning\b"],
    "nlp": [r"\bnlp\b", r"\bnatural language processing\b"],
    "statistics": [r"\bstatistics\b", r"\bstatistical\b"],
    "data visualization": [r"\bdata visualization\b", r"\bvisualization\b"],
}

_SKILL_ANCHORS = (
    "required",
    "requirements",
    "qualification",
    "qualifications",
    "skills",
    "must have",
    "preferred",
)


def _extract_anchor_windows(text: str, window: int = 320) -> str:
    """Extract text windows around requirement/qualification/skills anchors."""
    lower = text.lower()
    windows = []
    for anchor in _SKILL_ANCHORS:
        start = 0
        while True:
            idx = lower.find(anchor, start)
            if idx == -1:
                break
            left = max(0, idx - window)
            right = min(len(text), idx + window)
            windows.append(text[left:right])
            start = idx + len(anchor)

    return " ".join(windows)


def parse_skills(raw_skills: str, description: str, limit: int = 10) -> str:
    """Extract normalized skills using anchor-focused + NLP-assisted matching."""
    base_text = normalize_text(f"{raw_skills} {description}")
    if not base_text:
        return ""

    priority_text = normalize_text(_extract_anchor_windows(base_text))
    candidate_text = f"{priority_text} {base_text}".lower()

    found = []
    for skill, patterns in _SKILL_ALIASES.items():
        for pattern in patterns:
            if re.search(pattern, candidate_text):
                found.append(skill)
                break

    # NLP fallback: capture noun chunks/entities around anchor windows and match aliases.
    if _nlp and len(found) < limit:
        nlp_input = priority_text if priority_text else base_text
        doc = _nlp(nlp_input.lower())
        phrases = set()

        for chunk in doc.noun_chunks:
            text = normalize_text(chunk.text)
            if 1 <= len(text.split()) <= 4:
                phrases.add(text)

        for ent in doc.ents:
            text = normalize_text(ent.text)
            if 1 <= len(text.split()) <= 4:
                phrases.add(text)

        for phrase in phrases:
            for skill, patterns in _SKILL_ALIASES.items():
                if skill in found:
                    continue
                for pattern in patterns:
                    if re.search(pattern, phrase):
                        found.append(skill)
                        break
                if len(found) >= limit:
                    break
            if len(found) >= limit:
                break

    return ", ".join(found[:limit])
