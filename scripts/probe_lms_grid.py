"""End-to-end LMS scrape probe.

Logs in, opens the LMS Index page, fills From/To with today's date,
clicks Search, then waits 25s while taking screenshots so we can see
what the portal actually shows on this server (results / empty / captcha).

Outputs:
  /root/Amity_Primary/lms_after_login.png
  /root/Amity_Primary/lms_after_search.png
  /root/Amity_Primary/lms_after_search.html

Run on the server:
  cd /root/Amity_Primary && TZ=Asia/Kolkata \
    ./.venv/bin/python scripts/probe_lms_grid.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
from playwright.async_api import async_playwright

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(ROOT, ".env"))

LOGIN_URL = (
    os.environ.get("PORTAL_LOGIN_URL")
    or os.environ.get("PORTAL_URL")
    or "https://portal.amity.edu/Lead"
)
SEARCH_URL = (
    os.environ.get("SCRAPER_SEARCH_PAGE_URL")
    or "https://portal.amity.edu/Lead/LMS/Index"
)
USERNAME = os.environ.get("PORTAL_USERNAME") or ""
PASSWORD = os.environ.get("PORTAL_PASSWORD") or ""
EMAIL_SEL = os.environ.get(
    "SCRAPER_EMAIL_SELECTOR",
    'input[name="UserName"],#UserName',
)
PASS_SEL = os.environ.get(
    "SCRAPER_PASSWORD_SELECTOR",
    'input[name="Password"],#Password',
)
SUBMIT_SEL = os.environ.get(
    "SCRAPER_LOGIN_SUBMIT_SELECTOR",
    'button[type="submit"],input[type="submit"]',
)
SEARCH_DATA_SEL = os.environ.get(
    "SCRAPER_SEARCH_DATA_SELECTOR",
    'button:has-text("Search Data"),a:has-text("Search Data")',
)
DATE_FROM_SEL = os.environ.get(
    "SCRAPER_DATE_FROM_SELECTOR",
    "#searchFromDate,input[name='searchFromDate']",
)
DATE_TO_SEL = os.environ.get(
    "SCRAPER_DATE_TO_SELECTOR",
    "#searchToDate,input[name='searchToDate']",
)
RESULTS_SEARCH_SEL = os.environ.get(
    "SCRAPER_RESULTS_SEARCH_BUTTON_SELECTOR",
    "#btnSearch",
)
TABLE_SEL = os.environ.get("SCRAPER_RESULTS_TABLE_SELECTOR", "#divLMSPartial table")

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


async def first_match(page, comma_separated: str):
    for sel in [s.strip() for s in (comma_separated or "").split(",") if s.strip()]:
        loc = page.locator(sel).first
        try:
            if await loc.count() > 0:
                return sel, loc
        except Exception:
            continue
    return None, None


def today_dd_mm_yyyy() -> str:
    ist = timezone(timedelta(hours=5, minutes=30))
    return datetime.now(ist).strftime("%d/%m/%Y")


async def main() -> int:
    if not USERNAME or not PASSWORD:
        print("MISSING_CREDS")
        return 2
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(
            user_agent=UA, viewport={"width": 1366, "height": 900}
        )
        page = await context.new_page()

        await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
        e_sel, e_loc = await first_match(page, EMAIL_SEL)
        p_sel, p_loc = await first_match(page, PASS_SEL)
        if not e_loc or not p_loc:
            print("NO_LOGIN_FORM_FOUND")
            await browser.close()
            return 3
        await e_loc.fill(USERNAME)
        await p_loc.fill(PASSWORD)
        s_sel, s_loc = await first_match(page, SUBMIT_SEL)
        if s_loc:
            await s_loc.click()
        else:
            await p_loc.press("Enter")
        try:
            await page.wait_for_load_state("networkidle", timeout=30000)
        except Exception as exc:
            print("WAIT_AFTER_LOGIN:", type(exc).__name__, str(exc))

        print("AFTER_LOGIN_URL:", page.url)
        print("AFTER_LOGIN_TITLE:", await page.title())
        await page.screenshot(
            path=os.path.join(ROOT, "lms_after_login.png"), full_page=True
        )

        await page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=60000)
        try:
            await page.wait_for_load_state("networkidle", timeout=20000)
        except Exception:
            pass
        print("LMS_URL:", page.url)
        print("LMS_TITLE:", await page.title())

        sd_sel, sd_loc = await first_match(page, SEARCH_DATA_SEL)
        if sd_loc:
            print("CLICK Search Data:", sd_sel)
            try:
                await sd_loc.click()
                await page.wait_for_timeout(1500)
            except Exception as exc:
                print("SEARCH_DATA_CLICK_ERR:", exc)

        f_sel, f_loc = await first_match(page, DATE_FROM_SEL)
        t_sel, t_loc = await first_match(page, DATE_TO_SEL)
        print("DATE_SELECTORS:", {"from": f_sel, "to": t_sel})
        today = today_dd_mm_yyyy()
        if f_loc and t_loc:
            try:
                await f_loc.fill(today)
                await t_loc.fill(today)
                print("FILLED dates ->", today)
            except Exception as exc:
                print("DATE_FILL_ERR:", exc)
        else:
            print("DATE_INPUTS_NOT_FOUND")

        rs_sel, rs_loc = await first_match(page, RESULTS_SEARCH_SEL)
        print("RESULTS_SEARCH_SELECTOR:", rs_sel)
        if rs_loc:
            try:
                await rs_loc.click()
            except Exception as exc:
                print("RESULTS_SEARCH_CLICK_ERR:", exc)

        await page.wait_for_timeout(20000)

        try:
            count = await page.locator(TABLE_SEL).count()
        except Exception:
            count = 0
        print("TABLE_LOCATOR:", TABLE_SEL, "MATCHES:", count)
        try:
            row_count = (
                await page.locator(f"{TABLE_SEL} tbody tr").count() if count else 0
            )
        except Exception:
            row_count = 0
        print("TABLE_ROWS_VISIBLE:", row_count)

        empty_text = await page.evaluate(
            """
            () => {
              const txts = ['No records', 'No data', 'No record found', 'no record', 'no data'];
              const html = document.body ? document.body.innerText : '';
              for (const t of txts) {
                if (html && html.toLowerCase().includes(t.toLowerCase())) return t;
              }
              return '';
            }
            """
        )
        print("EMPTY_BANNER:", empty_text or "<none>")

        await page.screenshot(
            path=os.path.join(ROOT, "lms_after_search.png"), full_page=True
        )
        with open(
            os.path.join(ROOT, "lms_after_search.html"), "w", encoding="utf-8"
        ) as fh:
            fh.write(await page.content())

        await browser.close()
        return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
