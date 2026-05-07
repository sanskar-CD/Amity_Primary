"""Application settings from environment (no secrets in frontend)."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Support running the API from either the repo root or `backend/`.
    # When started from `backend/`, the root `.env` is one directory up.
    model_config = SettingsConfigDict(env_file=(".env", "../.env"), env_file_encoding="utf-8", extra="ignore")

    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_db: str = "amity_leads"

    # React dev server on this machine only (add more origins via CORS_ORIGINS in .env if needed).
    cors_origins: str = "http://localhost:5173"

    # Portal / scraper (server-side only)
    portal_url: str = ""
    portal_login_url: str = ""
    portal_username: str = ""
    portal_password: str = ""

    playwright_user_data_dir: str = "./data/playwright_profile"
    scraper_headless: bool = True
    scraper_timeout_ms: int = 60_000
    scraper_max_retries: int = 3
    scraper_retry_delay_sec: float = 2.0

    # Navigation & DOM (comma-separated = try first match in order)
    scraper_search_page_url: str = ""
    scraper_post_login_wait_ms: int = 1500

    scraper_email_selector: str = 'input[type="email"],input[name="email"],input[name="Email"],#email'
    scraper_password_selector: str = 'input[type="password"],input[name="password"],#password'
    scraper_login_submit_selector: str = 'button[type="submit"],input[type="submit"],button:has-text("Login"),button:has-text("Sign in")'

    scraper_date_input_selector: str = 'input[type="date"],input[name="date"],input[name="selectedDate"]'
    scraper_search_button_selector: str = 'button:has-text("Search"),input[value="Search"],button[type="submit"]'
    scraper_results_table_selector: str = "table"
    scraper_results_row_selector: str = "tbody tr"
    scraper_next_page_selector: str = 'a:has-text("Next"),button:has-text("Next"),.pagination .next'

    # Scheduler
    auto_sync_cron_hour: int = 6
    auto_sync_cron_minute: int = 0
    auto_sync_use_yesterday: bool = False


@lru_cache
def get_settings() -> Settings:
    return Settings()
