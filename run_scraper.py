import os
import yaml
import pandas as pd
from src.scraper import JobScraper
from src.storage import save_to_csv


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


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


    try:
        linkedin_jobs = scraper.scrape_linkedin(url_config["linkedin"]["url"], min_jobs=30)
        print(f"LinkedIn scraped {len(linkedin_jobs)} jobs")
        all_jobs.append(linkedin_jobs)
    finally:
        scraper.close()

    df = pd.concat(all_jobs, ignore_index=True)
    df = df.drop_duplicates(subset=["job_title", "company", "apply_link"])

    save_to_csv(df, output_config["csv"])
    print(f"Saved {len(df)} jobs to {output_config['csv']}")


if __name__ == "__main__":
    main()
