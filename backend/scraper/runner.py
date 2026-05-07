"""
Playwright flow: persistent session, login if needed, Search Data + date + scrape.
Selectors are driven by Settings — tune .env for your portal DOM.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Awaitable, Callable

from playwright.async_api import BrowserContext, Page, async_playwright

from config import Settings
from scraper.extract import click_next_if_present, extract_table_rows

logger = logging.getLogger(__name__)


async def _pick_locator(page: Page, comma_separated: str):
    for sel in [p.strip() for p in comma_separated.split(",") if p.strip()]:
        loc = page.locator(sel).first
        try:
            if await loc.count() > 0:
                return loc
        except Exception:
            continue
    return None


def _resolve_login_url(settings: Settings) -> str:
    base = (settings.portal_url or "").strip().rstrip("/")
    if settings.portal_login_url.strip():
        return settings.portal_login_url.strip()
    if base:
        return f"{base}/login"
    raise RuntimeError("portal_url or portal_login_url must be set")


def _resolve_search_url(settings: Settings) -> str:
    if settings.scraper_search_page_url.strip():
        u = settings.scraper_search_page_url.strip()
        if u.startswith("http"):
            return u
        base = (settings.portal_url or "").strip().rstrip("/")
        if not base:
            raise RuntimeError("portal_url required when scraper_search_page_url is relative")
        return f"{base}/{u.lstrip('/')}"
    base = (settings.portal_url or "").strip().rstrip("/")
    if not base:
        raise RuntimeError("portal_url or scraper_search_page_url must be configured")
    return base


async def _ensure_login(page: Page, settings: Settings) -> None:
    user = (settings.portal_username or "").strip()
    pw = (settings.portal_password or "").strip()
    if not user or not pw:
        raise RuntimeError("portal_username and portal_password must be set in environment")

    login_url = _resolve_login_url(settings)
    await page.goto(login_url, wait_until="domcontentloaded", timeout=settings.scraper_timeout_ms)

    email_loc = await _pick_locator(page, settings.scraper_email_selector)
    pass_loc = await _pick_locator(page, settings.scraper_password_selector)
    if email_loc and pass_loc:
        await email_loc.fill(user, timeout=settings.scraper_timeout_ms)
        await pass_loc.fill(pw, timeout=settings.scraper_timeout_ms)
        submit = await _pick_locator(page, settings.scraper_login_submit_selector)
        if submit:
            await submit.click(timeout=settings.scraper_timeout_ms)
        else:
            await pass_loc.press("Enter")
        # Avoid networkidle here: some portals keep long-polling open and never go idle.
        await page.wait_for_load_state("domcontentloaded", timeout=settings.scraper_timeout_ms)
    else:
        logger.info("Login form selectors not found; assuming already authenticated via storage state")

    await asyncio.sleep(settings.scraper_post_login_wait_ms / 1000.0)


async def _set_date_and_search(page: Page, settings: Settings, selected_date: str) -> None:
    search_url = _resolve_search_url(settings)
    await page.goto(search_url, wait_until="domcontentloaded", timeout=settings.scraper_timeout_ms)
    await asyncio.sleep(0.2)

    date_loc = await _pick_locator(page, settings.scraper_date_input_selector)
    if not date_loc:
        raise RuntimeError(
            "Date input not found. Set SCRAPER_DATE_INPUT_SELECTOR to match your portal (see .env.example)."
        )
    try:
        await date_loc.fill(selected_date, timeout=settings.scraper_timeout_ms)
    except Exception:
        await date_loc.click()
        await page.keyboard.type(selected_date)

    search_btn = await _pick_locator(page, settings.scraper_search_button_selector)
    if not search_btn:
        raise RuntimeError("Search button not found; adjust SCRAPER_SEARCH_BUTTON_SELECTOR")
    await search_btn.click(timeout=settings.scraper_timeout_ms)
    # Wait for the results table rather than networkidle (some portals never go idle).
    await page.wait_for_selector(settings.scraper_results_table_selector, state="attached", timeout=settings.scraper_timeout_ms)
    await asyncio.sleep(0.2)


async def scrape_leads_for_date(
    settings: Settings,
    selected_date: str,
    *,
    on_page_rows: Callable[[list[dict[str, Any]]], Awaitable[None]] | None = None,
) -> list[dict[str, Any]]:
    user_data = Path(settings.playwright_user_data_dir)
    user_data.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict[str, Any]] = []
    async with async_playwright() as p:
        context: BrowserContext = await p.chromium.launch_persistent_context(
            user_data_dir=str(user_data.resolve()),
            headless=settings.scraper_headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        # Speed-up: block heavy resources that don't affect table text extraction.
        try:
            async def _route_handler(route):
                rt = route.request.resource_type
                if rt in ("image", "font", "media"):
                    await route.abort()
                else:
                    await route.continue_()

            await context.route("**/*", _route_handler)
        except Exception:
            # If routing fails for any reason, continue without it.
            pass

        page = context.pages[0] if context.pages else await context.new_page()
        page.set_default_timeout(settings.scraper_timeout_ms)

        try:
            await _ensure_login(page, settings)
            await _set_date_and_search(page, settings, selected_date)

            prev_signature: str | None = None
            stagnant = 0
            while True:
                rows = await extract_table_rows(
                    page,
                    settings.scraper_results_table_selector,
                    settings.scraper_results_row_selector,
                )
                sig = str(len(rows)) + "|" + (rows[0].get(list(rows[0].keys())[0], "") if rows else "")
                if sig == prev_signature:
                    stagnant += 1
                    if stagnant >= 2:
                        break
                else:
                    stagnant = 0
                prev_signature = sig
                if on_page_rows is not None and rows:
                    await on_page_rows(rows)
                all_rows.extend(rows)

                advanced = await click_next_if_present(page, settings.scraper_next_page_selector)
                if not advanced:
                    break
                await page.wait_for_load_state("domcontentloaded", timeout=settings.scraper_timeout_ms)
                await page.wait_for_selector(
                    settings.scraper_results_table_selector, state="attached", timeout=settings.scraper_timeout_ms
                )
                await asyncio.sleep(0.2)
        finally:
            await context.close()

    return all_rows


async def scrape_with_retries(
    settings: Settings,
    selected_date: str,
    *,
    on_page_rows: Callable[[list[dict[str, Any]]], Awaitable[None]] | None = None,
    on_attempt_begin: Callable[[], Awaitable[None]] | None = None,
) -> list[dict[str, Any]]:
    last_exc: Exception | None = None
    for attempt in range(1, settings.scraper_max_retries + 1):
        try:
            if on_attempt_begin is not None:
                await on_attempt_begin()
            return await scrape_leads_for_date(
                settings, selected_date, on_page_rows=on_page_rows
            )
        except Exception as exc:
            last_exc = exc
            logger.exception("Scrape attempt %s failed: %s", attempt, exc)
            if attempt < settings.scraper_max_retries:
                await asyncio.sleep(settings.scraper_retry_delay_sec * attempt)
    assert last_exc is not None
    raise last_exc
