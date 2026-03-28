from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
from time import sleep
from typing import Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from src.utils import parse_experience, parse_skills


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
        # Restrict to top-level cards only; nested containers caused duplicate records.
        card_selector = (
            "li.scaffold-layout__list-item[data-occludable-job-id], "
            "li[data-occludable-job-id], "
            "ul.jobs-search-results__list > li, "
            "ul.jobs-search__results-list > li"
        )

        jobs: List[Dict[str, str]] = []
        seen_links = set()
        page_size = 25
        max_pages = 8
        last_cards: List = []

        for page in range(max_pages):
            page_url = self._with_linkedin_start(url, page * page_size)
            self.driver.get(page_url)

            try:
                self.wait.until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "ul.jobs-search-results__list, ul.jobs-search__results-list")
                    )
                )
            except Exception:
                pass

            self._scroll_page(page_size, card_selector)

            cards = self.driver.find_elements(By.CSS_SELECTOR, card_selector)
            last_cards = cards
            print(f"[DEBUG] LinkedIn page {page + 1} card count: {len(cards)}")

            for idx in range(len(cards)):
                current_cards = self.driver.find_elements(By.CSS_SELECTOR, card_selector)
                if idx >= len(current_cards):
                    continue

                card = current_cards[idx]
                title = self._safe_text(
                    card,
                    "a.job-card-container__link span[aria-hidden='true'], .artdeco-entity-lockup__title span[aria-hidden='true'], h3.base-search-card__title"
                )
                company = self._safe_text(
                    card,
                    ".artdeco-entity-lockup__subtitle span[aria-hidden='true'], .job-card-container__primary-description, h4.base-search-card__subtitle"
                )
                location = self._safe_text(
                    card,
                    ".job-card-container__metadata-item, li.job-card-container__metadata-wrapper li, span.job-search-card__location"
                )

                card_lines = self._read_card_lines(card)
                if not title and card_lines:
                    title = card_lines[0]
                if not company:
                    company = self._pick_company_from_lines(card_lines)
                if not location:
                    location = self._pick_location_from_lines(card_lines)

                link = self._safe_attribute(
                    card,
                    "a.job-card-container__link[href], a.base-card__full-link[href], a[href*='/jobs/view/']",
                    "href"
                )
                if not link:
                    try:
                        first_anchor = card.find_element(By.CSS_SELECTOR, "a[href*='/jobs/view/']")
                        link = first_anchor.get_attribute("href") or ""
                    except Exception:
                        link = ""

                if link and link.startswith("/"):
                    link = "https://www.linkedin.com" + link
                if link and "?" in link:
                    link = link.split("?", 1)[0]

                print(f"[DEBUG] LinkedIn card: title={title!r}, company={company!r}, location={location!r}, link={link!r}")
                if not link or link in seen_links:
                    continue
                seen_links.add(link)

                posted_date = self._safe_text(
                    card,
                    "time, span.job-search-card__listdate, span.job-search-card__listdate--new, span.job-posted-date"
                )

                details = self._extract_linkedin_details_panel(card)
                title = details.get("job_title") or title
                company = details.get("company") or company
                location = details.get("location") or location
                details["posted_date"] = details.get("posted_date") or posted_date

                job_id = re.search(r"/jobs/view/(\d+)", link)
                normalized = self._normalize_linkedin_job(
                    {
                        "job_id": job_id.group(1) if job_id else "",
                        "platform": "LinkedIn",
                        "job_title": title,
                        "company": company,
                        "location": location,
                        "experience_required": details.get("experience_required", ""),
                        "skills": details.get("skills", ""),
                        "job_description": details.get("job_description", ""),
                        "apply_link": link,
                        "posted_date": details.get("posted_date", ""),
                    }
                )

                jobs.append(normalized)
                if len(jobs) >= min_jobs:
                    break

            if len(jobs) >= min_jobs:
                break

        if not jobs:
            self._dump_linkedin_debug(last_cards)

        jobs = sorted(jobs, key=lambda job: self._posted_date_rank(job.get("posted_date", "")))

        return pd.DataFrame(jobs, columns=[
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
        ])

    def _extract_linkedin_details_panel(self, card) -> Dict[str, str]:
        """Click a result card and parse details shown in the right-side panel."""
        # Extract the expected job ID from the card link so we can verify the panel switches.
        expected_job_id: str = ""
        try:
            link_href = self._safe_attribute(card, "a[href*='/jobs/view/']", "href")
            m = re.search(r"/jobs/view/(\d+)", link_href)
            if m:
                expected_job_id = m.group(1)
        except Exception:
            pass

        try:
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", card)
            card_link = card.find_element(
                By.CSS_SELECTOR,
                "a.job-card-container__link[href], a.base-card__full-link[href], a[href*='/jobs/view/']"
            )
            self.driver.execute_script("arguments[0].click();", card_link)
        except Exception:
            try:
                self.driver.execute_script("arguments[0].click();", card)
            except Exception:
                try:
                    card.click()
                except Exception:
                    pass

        # Wait until LinkedIn updates currentJobId in the URL (confirms the right panel loaded).
        if expected_job_id:
            try:
                WebDriverWait(self.driver, 6).until(
                    lambda d: expected_job_id in d.current_url
                )
            except TimeoutException:
                sleep(1.0)
            if expected_job_id not in self.driver.current_url:
                return {
                    "job_title": "",
                    "company": "",
                    "location": "",
                    "job_description": "",
                    "experience_required": "",
                    "skills": "",
                    "posted_date": "",
                }
        else:
            sleep(1.0)

        # Locate the right-side detail panel to scope all reads.
        # This prevents picking up company/location text from the job-list on the LEFT side,
        # which caused the "all rows get Turing/Flipkart" bug.
        try:
            panel = self.driver.find_element(By.CSS_SELECTOR,
                ".scaffold-layout__detail, "
                ".jobs-search__job-details--wrapper, "
                ".jobs-details, "
                ".job-view-layout"
            )
        except Exception:
            panel = self.driver

        # Wait for the description content to be visible inside the panel.
        try:
            WebDriverWait(self.driver, 6).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR,
                    "div#job-details, div.jobs-description-content__text, "
                    "div.show-more-less-html__markup, div.jobs-box__html-content"))
            )
        except TimeoutException:
            sleep(1.0)

        # Expand truncated descriptions when a "Show more" button exists.
        try:
            see_more = panel.find_element(By.CSS_SELECTOR,
                "button.jobs-description__footer-button, "
                ".show-more-less-html__button--more")
            self.driver.execute_script("arguments[0].click();", see_more)
            sleep(0.4)
        except Exception:
            pass

        # All reads are scoped to `panel` (right side only) to prevent cross-contamination
        # with job-list elements on the left side of the page.
        panel_title = self._safe_text(
            panel,
            "h1.t-24, h2.t-24, a[data-test-id='job-details-job-title'], "
            ".jobs-unified-top-card__job-title, "
            ".job-details-jobs-unified-top-card__job-title h1"
        )
        panel_company = self._safe_first_valid_text(
            panel,
            [
                "a.app-aware-link[href*='/company/']",
                ".job-details-jobs-unified-top-card__company-name a",
                "div.jobs-unified-top-card__company-name a",
                ".jobs-unified-top-card__company-name",
                ".job-details-jobs-unified-top-card__company-name",
            ]
        )
        panel_location = self._safe_first_valid_text(
            panel,
            [
                ".job-details-jobs-unified-top-card__primary-description-without-tagline",
                ".jobs-unified-top-card__primary-description .t-black--light",
                ".job-details-jobs-unified-top-card__primary-description .tvm__text",
                ".jobs-unified-top-card__workplace-type",
                ".tvm__text.tvm__text--low-emphasis",
            ],
            validator=self._looks_like_location
        )
        description = self._safe_text(
            panel,
            "div#job-details, "
            "div.jobs-description-content__text, "
            "div.show-more-less-html__markup, "
            ".jobs-description-content__text--stretch, "
            "div.jobs-box__html-content, "
            "div.jobs-description__content, "
            "article.jobs-description__container"
        )
        raw_experience = self._safe_text(
            panel,
            "li.jobs-unified-top-card__job-insight, span.jobs-unified-top-card__job-insight, "
            "li.job-criteria__item span.job-criteria__text"
        )
        raw_skills = self._safe_text(
            panel,
            "div.job-details-how-you-match__skills-item-subtitle, "
            "div.jobs-skill-match-status-list, "
            "ul.job-details-skill-match-status-list"
        )
        posted_date = self._safe_text(
            panel,
            "span.jobs-unified-top-card__posted-date, span.tvm__text, span.posted-time-ago__text"
        )

        # Guardrail: if panel is not showing the clicked job yet, ignore panel fields.
        panel_link = self._safe_attribute(
            panel,
            "a.jobs-apply-button[href*='/jobs/view/'], a[href*='/jobs/view/'][data-control-name], a[href*='/jobs/view/']",
            "href"
        )
        panel_job_id_match = re.search(r"/jobs/view/(\d+)", panel_link or "")
        panel_job_id = panel_job_id_match.group(1) if panel_job_id_match else ""
        if expected_job_id and panel_job_id and panel_job_id != expected_job_id:
            return {
                "job_title": "",
                "company": "",
                "location": "",
                "job_description": "",
                "experience_required": "",
                "skills": "",
                "posted_date": "",
            }

        # Try dedicated experience element first; fall back to scanning the full description.
        experience = parse_experience(raw_experience) or parse_experience(description)
        skills = self._extract_skills(raw_skills, description)

        return {
            "job_title": self._normalize_whitespace(panel_title),
            "company": self._normalize_whitespace(panel_company),
            "location": self._normalize_whitespace(panel_location),
            "job_description": self._normalize_whitespace(description),
            "experience_required": self._normalize_whitespace(experience),
            "skills": self._normalize_whitespace(skills),
            "posted_date": self._normalize_whitespace(posted_date),
        }

    def _read_card_lines(self, card) -> List[str]:
        try:
            lines = [line.strip() for line in card.text.splitlines() if line.strip()]
        except StaleElementReferenceException:
            return []

        return [self._normalize_whitespace(line) for line in lines]

    def _pick_company_from_lines(self, lines: List[str]) -> str:
        for line in lines[1:6]:
            if self._is_location_noise(line):
                continue
            if self._looks_like_posted_date(line):
                continue
            if self._is_company_noise(line):
                continue
            return line
        return ""

    def _pick_location_from_lines(self, lines: List[str]) -> str:
        for line in lines:
            if self._looks_like_location(line):
                return line
        return ""

    def _looks_like_location(self, value: str) -> bool:
        text = (value or "").lower()
        if not text or self._is_location_noise(text):
            return False

        location_markers = [
            "bengaluru", "bangalore", "india", "hyderabad", "pune", "mumbai", "delhi",
            "gurgaon", "remote", "on-site", "hybrid",
        ]
        return any(marker in text for marker in location_markers) or "," in text

    def _is_location_noise(self, value: str) -> bool:
        text = (value or "").lower().strip()
        noise = [
            "easy apply",
            "promoted",
            "actively recruiting",
            "be an early applicant",
            "with verification",
        ]
        return text in noise or any(item in text for item in noise)

    def _is_company_noise(self, value: str) -> bool:
        text = (value or "").lower().strip()
        return self._looks_like_posted_date(text) or self._is_location_noise(text)

    def _looks_like_posted_date(self, value: str) -> bool:
        text = (value or "").lower()
        return bool(re.search(r"\b(\d+\s*(hour|day|week|month|minute)s?\s*ago|just now)\b", text))

    def _extract_skills(self, skills_text: str, description: str) -> str:
        """Extract skills using NLP + requirement-anchor focused parsing."""
        return parse_skills(skills_text, description, limit=10)

    def _normalize_whitespace(self, value: str) -> str:
        return " ".join((value or "").split()).strip()

    def _safe_first_valid_text(self, parent, selectors: List[str], validator=None) -> str:
        """Try selectors in order and return the first non-empty result that passes an optional validator."""
        for selector in selectors:
            try:
                element = parent.find_element(By.CSS_SELECTOR, selector)
                text = element.text.strip()
                if text and (validator is None or validator(text)):
                    return text
            except Exception:
                continue
        return ""

    def _normalize_linkedin_job(self, job: Dict[str, str]) -> Dict[str, str]:
        """Normalize LinkedIn row values and guarantee all required fields are populated."""
        normalized = {key: self._normalize_whitespace(value) for key, value in job.items()}

        normalized["job_title"] = self._clean_linkedin_text(normalized.get("job_title", ""))
        normalized["company"] = self._clean_linkedin_text(normalized.get("company", ""))
        normalized["location"] = self._clean_linkedin_text(normalized.get("location", ""))

        if self._is_location_noise(normalized["location"]):
            normalized["location"] = ""

        # Clear location when it mirrors the job title or is not a recognisable place.
        if normalized["location"] and normalized["location"].lower() == normalized["job_title"].lower():
            normalized["location"] = ""
        if normalized["location"] and not self._looks_like_location(normalized["location"]):
            normalized["location"] = ""

        # Clear company when it equals the job title (exact or normalised).
        if normalized["company"].lower() == normalized["job_title"].lower():
            normalized["company"] = ""
        # Clear company when it mirrors the location (common LinkedIn artefact).
        if normalized["company"] and normalized["company"].lower() == normalized["location"].lower():
            normalized["company"] = ""

        defaults = {
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

        for key, default in defaults.items():
            if not normalized.get(key):
                normalized[key] = default

        return normalized

    def _clean_linkedin_text(self, value: str) -> str:
        text = self._normalize_whitespace(value)
        text = re.sub(r"\s*with verification\b", "", text, flags=re.IGNORECASE)
        if "\n" in text:
            text = text.split("\n", 1)[0].strip()
        return self._normalize_whitespace(text)

    def _with_linkedin_start(self, url: str, start: int) -> str:
        """Set or replace LinkedIn start offset query param for pagination."""
        parsed = urlparse(url)
        params = dict(parse_qsl(parsed.query, keep_blank_values=True))
        params["start"] = str(start)
        new_query = urlencode(params)
        return urlunparse(parsed._replace(query=new_query))

    def _posted_date_rank(self, posted_date: str) -> int:
        """Lower score means more recent posting."""
        text = (posted_date or "").lower()
        if not text:
            return 10_000

        if "just now" in text:
            return 0

        match = re.search(r"(\d+)", text)
        amount = int(match.group(1)) if match else 1

        if "hour" in text:
            return amount
        if "day" in text:
            return amount * 24
        if "week" in text:
            return amount * 24 * 7
        if "month" in text:
            return amount * 24 * 30

        return 5_000

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

    def _scroll_page(self, min_jobs: int, card_selector: str) -> None:
        """Scroll and click LinkedIn load-more controls until enough cards are loaded."""
        previous_count = 0
        unchanged_rounds = 0

        for _ in range(45):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            sleep(1.2)

            current_count = len(self.driver.find_elements(By.CSS_SELECTOR, card_selector))
            print(f"[DEBUG] LinkedIn scroll card count: {current_count}")

            if current_count >= min_jobs:
                break

            if current_count == previous_count:
                unchanged_rounds += 1
                self._click_linkedin_load_more()
                sleep(1)
            else:
                unchanged_rounds = 0

            if unchanged_rounds >= 5:
                # Reached end of list or no further lazy-load growth.
                break

            previous_count = current_count

    def _click_linkedin_load_more(self) -> None:
        """Click LinkedIn's load-more button when present to fetch additional cards."""
        selectors = [
            "button.infinite-scroller__show-more-button",
            "button[aria-label*='See more jobs']",
            "button[aria-label*='Show more jobs']",
            "button[data-test-id='load-more-button']",
        ]

        for selector in selectors:
            try:
                button = self.driver.find_element(By.CSS_SELECTOR, selector)
                if button.is_displayed() and button.is_enabled():
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                    self.driver.execute_script("arguments[0].click();", button)
                    print("[DEBUG] LinkedIn load-more button clicked")
                    return
            except Exception:
                continue

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
        skills = self._safe_text(self.driver, "div.skills-section, ul.key-skills, div.tags")
        posted_date = self._safe_text(self.driver, "span.posted-time-ago__text, div.posted-date")

        experience = parse_experience(raw_experience) or parse_experience(description)

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
