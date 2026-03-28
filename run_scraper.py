import yaml
import pandas as pd
from pathlib import Path
from src.scraper import JobScraper
from src.storage import save_to_csv, save_to_sqlite


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def main() -> None:
    config = load_config("config.yaml")
    url_config = config["platforms"]
    output_config = config["output"]

    scraper = JobScraper(headless=config["browser"].get("headless", False))
    all_jobs = []

    try:
        all_jobs.append(scraper.scrape_linkedin(url_config["linkedin"]["url"], min_jobs=30))
        all_jobs.append(scraper.scrape_naukri(url_config["naukri"]["url"], min_jobs=30))
        all_jobs.append(scraper.scrape_indeed(url_config["indeed"]["url"], min_jobs=30))
    finally:
        scraper.close()

    df = pd.concat(all_jobs, ignore_index=True)
    df = df.drop_duplicates(subset=["job_title", "company", "apply_link"])

    save_to_csv(df, output_config["csv"])
    save_to_sqlite(df, output_config["sqlite"])
    print(f"Saved {len(df)} jobs to {output_config['csv']} and {output_config['sqlite']}")


if __name__ == "__main__":
    main()
