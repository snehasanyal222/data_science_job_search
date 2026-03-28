import os
import re
import yaml
import pandas as pd
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from src.scraper import JobScraper
from src.storage import save_to_csv


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def build_relaxed_linkedin_url(url: str) -> str:
    """Relax strict filters when first scrape returns too few jobs."""
    parsed = urlparse(url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))

    # Remove strict experience filter; keep keyword/location and recent sort.
    params.pop("f_E", None)
    params["sortBy"] = "DD"
    params.setdefault("f_TPR", "r2592000")

    new_query = urlencode(params)
    return urlunparse(parsed._replace(query=new_query))


# Compiled pattern of job-title substrings that indicate non-data-science roles.
_EXCLUDE_TITLE_RE = re.compile(
    r"full[\s\-]?stack"
    r"|front[\s\-]?end\s+(developer|engineer)"
    r"|back[\s\-]?end\s+(developer|engineer)"
    r"|\bdevops\b"
    r"|\bqa\s+(engineer|analyst|tester)\b"
    r"|\btest(ing)?\s+(engineer|lead|manager)\b"
    r"|\bsre\b"
    r"|\bembedded\s+(engineer|developer)\b"
    r"|\bfirmware\s+engineer\b"
    r"|\bandroid\s+(developer|engineer)\b"
    r"|\bios\s+(developer|engineer)\b"
    r"|\bmobile\s+(developer|engineer)\b"
    r"|\bgame\s+(developer|engineer)\b"
    r"|\bweb\s+(developer|designer)\b"
    r"|\bpython\s+developer\b"
    r"|\bsoftware\s+developer\b"
    r"|\bsde\s*\d*\b"
    r"|\bnetwork\s+engineer\b"
    r"|\bsystem\s+administrator\b"
    r"|\bdatabase\s+administrator\b"
    r"|\bui\s+(developer|engineer)\b"
    r"|\bux\s+(designer|engineer)\b"
    r"|\bengineer\b",
    re.IGNORECASE,
)


def _is_relevant_job(title: str) -> bool:
    """Return False for job titles that are clearly not data-science roles."""
    return not bool(_EXCLUDE_TITLE_RE.search(title or ""))


def main() -> None:
    config = load_config("config.yaml")
    url_config = config["platforms"]
    output_config = config["output"]

    scraper = JobScraper(headless=config["browser"].get("headless", False))
    all_jobs = []

    linkedin_username = (
        url_config["linkedin"].get("username")
        or os.environ.get("LINKEDIN_USERNAME")
    )
    linkedin_password = (
        url_config["linkedin"].get("password")
        or os.environ.get("LINKEDIN_PASSWORD")
    )

    if linkedin_username and linkedin_password:
        print("Logging in to LinkedIn before scraping...")
        scraped = scraper.login_linkedin(linkedin_username, linkedin_password)
        if not scraped:
            print("Automatic login failed. Opening manual login page.")
            scraper.open_linkedin_login()
            if scraper.wait_for_linkedin_login():
                print("Manual LinkedIn login detected.")
            else:
                print("LinkedIn login was not detected within timeout. Continuing without login.")
    else:
        print("LinkedIn credentials not configured. Opening browser for manual login.")
        scraper.open_linkedin_login()
        if scraper.wait_for_linkedin_login():
            print("Manual LinkedIn login detected.")
        else:
            print("LinkedIn login was not detected within timeout. Continuing without login.")


    required_columns = [
        "job_id",
        "platform",
        "job_title",
        "company",
        "location",
        "experience_required",
        "skills",
        "job_description",
        "apply_link",
        "posted_date",
    ]

    fill_defaults = {
        "job_id": "",
        "platform": "LinkedIn",
        "job_title": "Data Scientist",
        "company": "Unknown company",
        "location": "Location not specified",
        "experience_required": "Not specified",
        "skills": "Not specified",
        "job_description": "Description not available",
        "apply_link": "Not available",
        "posted_date": "Recently posted",
    }

    target_min_jobs = 20
    try:
        primary_url = url_config["linkedin"]["url"]
        linkedin_jobs = scraper.scrape_linkedin(primary_url, min_jobs=60)
        print(f"LinkedIn primary scrape captured {len(linkedin_jobs)} jobs")

        if len(linkedin_jobs) < target_min_jobs:
            relaxed_url = build_relaxed_linkedin_url(primary_url)
            print("Primary result set too small, running relaxed LinkedIn query...")
            relaxed_jobs = scraper.scrape_linkedin(relaxed_url, min_jobs=60)
            print(f"LinkedIn relaxed scrape captured {len(relaxed_jobs)} jobs")
            linkedin_jobs = pd.concat([linkedin_jobs, relaxed_jobs], ignore_index=True)

        all_jobs.append(linkedin_jobs)
    finally:
        scraper.close()

    df = pd.concat(all_jobs, ignore_index=True)
    # Deduplicate on apply_link — each job URL is unique regardless of parsed title/company noise.
    df = df.drop_duplicates(subset=["apply_link"])

    # Remove clearly non-data-science roles (e.g. fullstack, SDE, Python developer).
    before_filter = len(df)
    df = df[df["job_title"].apply(_is_relevant_job)].reset_index(drop=True)
    removed = before_filter - len(df)
    if removed:
        print(f"[INFO] Filtered out {removed} non-data-science jobs by title.")

    for column in required_columns:
        if column not in df.columns:
            df[column] = ""

    df = df[required_columns]
    df = df.fillna("")
    for column, default_value in fill_defaults.items():
        df[column] = df[column].astype(str).str.strip()
        df[column] = df[column].replace("", default_value)

    if len(df) < target_min_jobs:
        print(
            f"[WARN] Scraper produced only {len(df)} unique jobs after retries. "
            "Saving available jobs without failing the run."
        )

    save_to_csv(df, output_config["csv"])
    print(f"Saved {len(df)} jobs to {output_config['csv']}")


if __name__ == "__main__":
    main()
