from __future__ import annotations

from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Dict, List, Optional
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from src.utils import parse_experience, parse_salary


class JobScraper:
    """Simple Selenium scraper for LinkedIn."""

    def __init__(self, headless: bool = False) -> None:
        options = Options()
        options.add_argument("--window-size=1920,1080")
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        chrome_service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=chrome_service, options=options)
        self.driver.set_page_load_timeout(30)
        self.wait = WebDriverWait(self.driver, 15)

    def close(self) -> None:
        """Close the browser cleanly."""
        self.driver.quit()

    def login_linkedin(self, username: str, password: str) -> bool:
        """Log in to LinkedIn automatically with provided credentials."""
        print("[DEBUG] LinkedIn login: opening login page")
        self.driver.get("https://www.linkedin.com/login")
        try:
            username_field = self.wait.until(EC.visibility_of_element_located((By.ID, "username")))
            print("[DEBUG] LinkedIn login: username field visible")
            print("[DEBUG] LinkedIn login: entering username")
            username_field.send_keys(username)
            print("[DEBUG] LinkedIn login: finding password field")
            password_field = self.driver.find_element(By.ID, "password")
            print("[DEBUG] LinkedIn login: entering password")
            password_field.send_keys(password)
            print("[DEBUG] LinkedIn login: finding submit button")
            submit_button = self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
            print("[DEBUG] LinkedIn login: clicking submit")
            submit_button.click()
            print("[DEBUG] LinkedIn login: credentials submitted")
        except Exception as exc:
            print(f"[DEBUG] LinkedIn login: failed to submit credentials: {exc}")
            return False

        try:
            WebDriverWait(self.driver, 30).until(lambda d: self._is_linkedin_authenticated())
            print("[DEBUG] LinkedIn login: authenticated")
            return True
        except Exception as exc:
            print(f"[DEBUG] LinkedIn login: authentication not confirmed: {exc}")
            return False

    def open_linkedin_login(self) -> None:
        """Open the LinkedIn login page in the browser for manual authentication."""
        self.driver.get("https://www.linkedin.com/login")
        try:
            self.wait.until(EC.visibility_of_element_located((By.ID, "username")))
        except Exception:
            pass

    def _is_linkedin_authenticated(self) -> bool:
        """Return True if the current browser session is logged in to LinkedIn."""
        if self.driver.get_cookie("li_at"):
            return True

        current_url = self.driver.current_url
        return any(token in current_url for token in ("/feed/", "/jobs", "/me/", "/profile"))

    def wait_for_linkedin_login(self, timeout: int = 600) -> bool:
        """Wait until the user has logged in to LinkedIn manually."""
        try:
            WebDriverWait(self.driver, timeout).until(lambda d: self._is_linkedin_authenticated())
            return True
        except Exception:
            return False

    def scrape_linkedin(self, url: str, min_jobs: int = 50) -> pd.DataFrame:
        """Scrape LinkedIn search results and return a job DataFrame."""
        self.driver.get(url)
        try:
            self.wait.until(
                EC.presence_of_element_located(
                    (
                        By.CSS_SELECTOR,
                        "ul.jobs-search__results-list, ul.jobs-search-results__list, li[data-occludable-job-id], div.base-card"
                    )
                )
            )
        except Exception:
            pass
        self._scroll_page(min_jobs)

        card_selector = (
            "ul.jobs-search__results-list > li, ul.jobs-search-results__list > li, "
            "li[data-occludable-job-id], div.job-card-container, div.job-search-card, div.base-card"
        )
        cards = self.driver.find_elements(
            By.CSS_SELECTOR,
            card_selector,
        )
        print(f"[DEBUG] LinkedIn card count: {len(cards)}")

        jobs: List[Dict[str, str]] = []
        max_cards = min(len(cards), min_jobs)
        for idx in range(max_cards):
            current_cards = self.driver.find_elements(By.CSS_SELECTOR, card_selector)
            if idx >= len(current_cards):
                continue

            card = current_cards[idx]
            title = self._safe_text(
                card,
                "h3.base-search-card__title, h3.job-card-list__title, a.base-card__full-link, a.job-card-list__title"
            )
            company = self._safe_text(
                card,
                "h4.base-search-card__subtitle, h4.job-card-container__company-name, a.hidden-nested-link, span.base-search-card__subtitle"
            )
            location = self._safe_text(
                card,
                "span.job-search-card__location, span.job-card-container__metadata-item, span.job-search-card__listdate--new, li.job-search-card__location"
            )

            if not title:
                title = self._safe_text(card, "a.job-card-list__title, a.job-card-container__link, div.job-card-list__title, span.jobs-details-top-card__job-title")
            if not company:
                company = self._safe_text(card, "span.job-card-container__company-name, div.job-card-container__company-name, span.job-card-company__name")
            if not location:
                location = self._safe_text(card, "span.job-card-list__location, span.job-card-container__metadata-item")

            if not title or not company or not location:
                card_text_lines: List[str] = []
                try:
                    card_text_lines = [line.strip() for line in card.text.splitlines() if line.strip()]
                except StaleElementReferenceException:
                    pass
                if card_text_lines:
                    if not title:
                        title = card_text_lines[0]
                    if not company and len(card_text_lines) > 1:
                        company = card_text_lines[1]
                    if not location and len(card_text_lines) > 2:
                        location = card_text_lines[-1]

            link = self._safe_attribute(
                card,
                "a.base-card__full-link, a.base-card__full-link[href], a.job-card-list__title, a.job-card-container__link, a[href*='/jobs/view/'], a[href*='/jobs/']",
                "href"
            )
            if not link:
                try:
                    first_anchor = card.find_element(By.CSS_SELECTOR, "a[href]")
                    link = first_anchor.get_attribute("href") or ""
                except Exception:
                    link = ""

            if link and link.startswith("/"):
                link = "https://www.linkedin.com" + link
            if link and "?" in link:
                link = link.split("?", 1)[0]

            print(f"[DEBUG] LinkedIn card: title={title!r}, company={company!r}, location={location!r}, link={link!r}")
            if not link:
                continue

            # Job page navigation is flaky on LinkedIn and can hang the scraper.
            # Use card-level metadata only for now and keep the apply link.
            details = {
                "experience_required": "",
                "skills": "",
                "salary": "",
                "job_description": "",
                "posted_date": "",
            }

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

        if not jobs:
            self._dump_linkedin_debug(cards)

        return pd.DataFrame(jobs, columns=[
            "platform",
            "job_title",
            "company",
            "location",
            "experience_required",
            "skills",
            "salary",
            "job_description",
            "apply_link",
            "posted_date",
        ])

    def _dump_linkedin_debug(self, cards: List) -> None:
        """Write page source, screenshot, and sample card HTML when zero jobs are extracted."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        debug_dir = Path("data") / "debug" / "linkedin" / timestamp
        debug_dir.mkdir(parents=True, exist_ok=True)

        page_path = debug_dir / "page_source.html"
        screenshot_path = debug_dir / "page_screenshot.png"
        cards_path = debug_dir / "cards_dump.txt"

        try:
            page_path.write_text(self.driver.page_source, encoding="utf-8")
        except Exception as exc:
            print(f"[DEBUG] Failed to write page source: {exc}")

        try:
            self.driver.save_screenshot(str(screenshot_path))
        except Exception as exc:
            print(f"[DEBUG] Failed to save screenshot: {exc}")

        try:
            with cards_path.open("w", encoding="utf-8") as handle:
                for idx, card in enumerate(cards[:5], start=1):
                    href = ""
                    try:
                        href = card.find_element(By.CSS_SELECTOR, "a[href]").get_attribute("href") or ""
                    except Exception:
                        pass

                    handle.write(f"===== CARD {idx} =====\n")
                    handle.write(f"href: {href}\n")
                    handle.write("text:\n")
                    handle.write((card.text or "") + "\n\n")
                    handle.write("outerHTML:\n")
                    handle.write((card.get_attribute("outerHTML") or "") + "\n\n")
        except Exception as exc:
            print(f"[DEBUG] Failed to write card dump: {exc}")

        print(f"[DEBUG] LinkedIn debug artifacts saved to {debug_dir}")

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
        """Navigate to a job page and collect details, then return to the search results."""
        try:
            self.driver.get(url)
        except TimeoutException:
            pass

        try:
            self.wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div.show-more-less-html__markup, div.job-description, div.jd-desc")
                )
            )
        except Exception:
            pass

        description = self._safe_text(
            self.driver,
            "div.show-more-less-html__markup, div.job-description, div.jd-desc, div.description__text"
        )
        raw_experience = self._safe_text(self.driver, "span.job-criteria__text, li.job-criteria__item div")
        raw_salary = self._safe_text(self.driver, "span.salary, div.salary")
        skills = self._safe_text(self.driver, "div.skills-section, ul.key-skills, div.tags")
        posted_date = self._safe_text(self.driver, "span.posted-time-ago__text, div.posted-date")

        experience = parse_experience(raw_experience or description)
        salary = parse_salary(raw_salary or description)

        try:
            self.driver.back()
            self.wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "ul.jobs-search__results-list, ul.jobs-search-results__list")
                )
            )
        except Exception:
            pass

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
