import streamlit as st
import pandas as pd
from pathlib import Path

DATA_PATH = Path("data/jobs.csv")


def load_jobs() -> pd.DataFrame:
    return pd.read_csv(DATA_PATH)


def show_summary(df: pd.DataFrame) -> None:
    st.markdown("### Summary")
    st.metric("Total jobs", len(df))

    if "platform" in df.columns:
        platform_counts = df["platform"].value_counts()
        st.bar_chart(platform_counts)

    st.markdown("#### Top companies")
    st.bar_chart(df["company"].value_counts().head(10))

    if "location" in df.columns:
        st.markdown("#### Location distribution")
        st.bar_chart(df["location"].value_counts().head(10))

    if "experience_required" in df.columns:
        st.markdown("#### Experience filter")
        st.bar_chart(df["experience_required"].value_counts().head(10))


def show_skills(df: pd.DataFrame) -> None:
    if "skills" not in df.columns:
        return

    st.markdown("### Skill counts")
    skill_series = df["skills"].dropna().astype(str)
    all_skills = skill_series.str.split(",").explode().str.strip()
    all_skills = all_skills[all_skills != ""]
    st.bar_chart(all_skills.value_counts().head(15))


def main() -> None:
    st.title("Data Science Job Market Analyzer")
    st.write(
        "Load scraped job data and display a simple report for LinkedIn and Naukri."
    )

    if not DATA_PATH.exists():
        st.warning("No data found yet. Run `python run_scraper.py` first.")
        return

    df = load_jobs()
    st.dataframe(df.head(20))

    show_summary(df)
    show_skills(df)

    if st.button("Download CSV"):
        st.download_button(
            label="Download job data as CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="jobs_report.csv",
            mime="text/csv",
        )

    st.markdown("---")
    st.markdown("#### Data quality notes")
    st.write(
        "The report uses the latest scraped CSV. If some fields are empty, update selectors in `src/scraper.py`."
    )


if __name__ == "__main__":
    main()
