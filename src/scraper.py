from __future__ import annotations

from time import sleep
from typing import Dict, List
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


class JobScraper:
    """Simple Selenium scraper for LinkedIn, Naukri, and Indeed."""

    def __init__(self, headless: bool = False) -> None:
        options = Options()
        options.add_argument("--window-size=1920,1080")
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        self.driver = webdriver.Chrome(
            ChromeDriverManager().install(),
            options=options,
        )
        self.wait = WebDriverWait(self.driver, 15)

    def close(self) -> None:
        """Close the browser cleanly."""
        self.driver.quit()

    def scrape_linkedin(self, url: str, min_jobs: int = 50) -> pd.DataFrame:
        """Scrape LinkedIn search results and return a job DataFrame."""
        self.driver.get(url)
        self._scroll_page(min_jobs)

        cards = self.driver.find_elements(
            By.CSS_SELECTOR,
            "ul.jobs-search__results-list li, ul.jobs-search-results__list li"
        )

        jobs: List[Dict[str, str]] = []
        for card in cards[:min_jobs]:
            title = self._safe_text(card, "h3.base-search-card__title, h3.job-card-list__title")
            company = self._safe_text(card, "h4.base-search-card__subtitle, h4.job-card-container__company-name")
            location = self._safe_text(card, "span.job-search-card__location, span.job-card-container__metadata-item")
            link = self._safe_attribute(card, "a.base-card__full-link, a.job-card-list__title", "href")

            if not link:
                continue

            details = self._scrape_job_page(link)
            jobs.append({
                "platform": "LinkedIn",
                "job_title": title,
                "company": company,
                "location": location,
                "experience_required": details.get("experience_required", ""),
                "skills": details.get("skills", ""),
                "salary": details.get("salary", ""),
                "job_description": details.get("job_description", ""),
                "apply_link": link,
                "posted_date": details.get("posted_date", ""),
            })

        return pd.DataFrame(jobs)

    def scrape_naukri(self, url: str, min_jobs: int = 50) -> pd.DataFrame:
        """Scrape Naukri search results and return a job DataFrame."""
        self.driver.get(url)
        self._scroll_page(min_jobs)

        cards = self.driver.find_elements(By.CSS_SELECTOR, "article.jobTuple")
        jobs: List[Dict[str, str]] = []

        for card in cards[:min_jobs]:
            title = self._safe_text(card, "a.title")
            company = self._safe_text(card, "a.subTitle")
            location = self._safe_text(card, "li.fleft.grey-text.br2.placeHolderLi.location")
            link = self._safe_attribute(card, "a.title", "href")

            if not link:
                continue

            details = self._scrape_job_page(link)
            jobs.append({
                "platform": "Naukri",
                "job_title": title,
                "company": company,
                "location": location,
                "experience_required": details.get("experience_required", ""),
                "skills": details.get("skills", ""),
                "salary": details.get("salary", ""),
                "job_description": details.get("job_description", ""),
                "apply_link": link,
                "posted_date": details.get("posted_date", ""),
            })

        return pd.DataFrame(jobs)

    def scrape_indeed(self, url: str, min_jobs: int = 50) -> pd.DataFrame:
        """Scrape Indeed search results and return a job DataFrame."""
        self.driver.get(url)
        self._scroll_page(min_jobs)

        cards = self.driver.find_elements(By.CSS_SELECTOR, "div.job_seen_beacon, a.tapItem")
        jobs: List[Dict[str, str]] = []

        for card in cards[:min_jobs]:
            title = self._safe_text(card, "h2.jobTitle, h2 span")
            company = self._safe_text(card, "span.companyName")
            location = self._safe_text(card, "div.companyLocation")
            link = self._safe_attribute(card, "a.jcs-JobTitle, a.tapItem", "href")
            if link and link.startswith("/"):
                link = "https://www.indeed.co.in" + link

            if not link:
                continue

            details = self._scrape_job_page(link)
            jobs.append({
                "platform": "Indeed",
                "job_title": title,
                "company": company,
                "location": location,
                "experience_required": details.get("experience_required", ""),
                "skills": details.get("skills", ""),
                "salary": details.get("salary", ""),
                "job_description": details.get("job_description", ""),
                "apply_link": link,
                "posted_date": details.get("posted_date", ""),
            })

        return pd.DataFrame(jobs)

    def _scroll_page(self, min_jobs: int) -> None:
        """Scroll until enough jobs are loaded or no more new cards appear."""
        previous_count = 0
        for _ in range(20):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            self.wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
            current_count = len(self.driver.find_elements(By.CSS_SELECTOR, "li, article, div.job_seen_beacon, a.tapItem"))
            if current_count >= min_jobs or current_count == previous_count:
                break
            previous_count = current_count
            sleep(1)

    def _scrape_job_page(self, url: str) -> Dict[str, str]:
        """Open a job page in a new tab and collect details."""
        self.driver.execute_script("window.open(arguments[0], '_blank');", url)
        self.driver.switch_to.window(self.driver.window_handles[-1])

        self.wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.show-more-less-html__markup, div.job-description, div.jd-desc")
            )
        )

        description = self._safe_text(
            self.driver,
            "div.show-more-less-html__markup, div.job-description, div.jd-desc"
        )
        experience = self._safe_text(self.driver, "span.job-criteria__text, li.job-criteria__item div")
        salary = self._safe_text(self.driver, "span.salary, div.salary")
        skills = self._safe_text(self.driver, "div.skills-section, ul.key-skills, div.tags")
        posted_date = self._safe_text(self.driver, "span.posted-time-ago__text, div.posted-date")

        self.driver.close()
        self.driver.switch_to.window(self.driver.window_handles[0])

        return {
            "job_description": description,
            "experience_required": experience,
            "skills": skills,
            "salary": salary,
            "posted_date": posted_date,
        }

    def _safe_text(self, parent, selector: str) -> str:
        """Return clean text for the first matching CSS selector."""
        try:
            element = parent.find_element(By.CSS_SELECTOR, selector)
            return element.text.strip()
        except Exception:
            return ""

    def _safe_attribute(self, parent, selector: str, attribute: str) -> str:
        """Return an attribute from the first matching element."""
        try:
            element = parent.find_element(By.CSS_SELECTOR, selector)
            return element.get_attribute(attribute) or ""
        except Exception:
            return ""
