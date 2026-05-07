"""End-to-end portal login probe.

Loads .env, navigates to PORTAL_LOGIN_URL, fills UserName/Password,
clicks the submit button, then prints what the page becomes:
  - HTTP status, final URL
  - Whether the login form is still visible
  - Visible error/alert text (first 500 chars)
  - First 30 input/button elements

Saves probe_after.png and probe_after.html in the project root.
Run on the server:
    cd /root/Amity_Primary && TZ=Asia/Kolkata \
        ./.venv/bin/python scripts/probe_full_login.py
"""
from __future__ import annotations

import asyncio
import os
import sys

from dotenv import load_dotenv
from playwright.async_api import async_playwright

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(ROOT, ".env"))

URL = os.environ.get("PORTAL_LOGIN_URL") or os.environ.get("PORTAL_URL") or "https://portal.amity.edu/Lead"
USERNAME = os.environ.get("PORTAL_USERNAME") or ""
PASSWORD = os.environ.get("PORTAL_PASSWORD") or ""
EMAIL_SEL = os.environ.get(
    "SCRAPER_EMAIL_SELECTOR",
    'input[name="UserName"],#UserName,input[type="email"]',
)
PASS_SEL = os.environ.get(
    "SCRAPER_PASSWORD_SELECTOR",
    'input[name="Password"],#Password,input[type="password"]',
)
SUBMIT_SEL = os.environ.get(
    "SCRAPER_LOGIN_SUBMIT_SELECTOR",
    'button[type="submit"],input[type="submit"]',
)
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


async def main() -> int:
    if not USERNAME or not PASSWORD:
        print("MISSING_CREDS: PORTAL_USERNAME or PORTAL_PASSWORD not loaded from .env")
        return 2
    print(
        "USING:",
        {
            "url": URL,
            "user_first2": USERNAME[:2] + "***",
            "pass_len": len(PASSWORD),
        },
    )

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(
            user_agent=UA,
            viewport={"width": 1366, "height": 900},
        )
        page = await context.new_page()

        try:
            response = await page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        except Exception as exc:
            print("GOTO_ERROR:", type(exc).__name__, str(exc))
            await browser.close()
            return 3
        print("PRE_STATUS:", response.status if response else None)
        print("PRE_URL   :", page.url)

        email_sel, email_loc = await first_match(page, EMAIL_SEL)
        pass_sel, pass_loc = await first_match(page, PASS_SEL)
        if not email_loc or not pass_loc:
            print("NO_LOGIN_FORM_FOUND")
            await browser.close()
            return 4
        print("FILLING:", {"email_selector": email_sel, "pass_selector": pass_sel})
        await email_loc.fill(USERNAME)
        await pass_loc.fill(PASSWORD)

        submit_sel, submit_loc = await first_match(page, SUBMIT_SEL)
        print("SUBMIT_SELECTOR:", submit_sel)

        nav = asyncio.create_task(
            page.wait_for_load_state("networkidle", timeout=30000)
        )
        if submit_loc:
            await submit_loc.click()
        else:
            await pass_loc.press("Enter")
        try:
            await nav
        except Exception as exc:
            print("WAIT_LOAD:", type(exc).__name__, str(exc))

        await page.wait_for_timeout(2500)

        print("POST_URL:", page.url)
        title = await page.title()
        print("POST_TITLE:", title)

        still_email = await page.locator(email_sel).count() if email_sel else 0
        still_pass = await page.locator(pass_sel).count() if pass_sel else 0
        print("LOGIN_FORM_STILL_VISIBLE:", bool(still_email and still_pass))

        try:
            error_text = await page.evaluate(
                """
                () => {
                  const sels = [
                    '.validation-summary-errors', '.field-validation-error',
                    '.alert', '.error', '.text-danger', '#errorMessage',
                  ];
                  for (const s of sels) {
                    const el = document.querySelector(s);
                    if (el && el.innerText && el.innerText.trim()) return el.innerText.trim();
                  }
                  return '';
                }
                """
            )
        except Exception:
            error_text = ""
        print("PAGE_ERROR_TEXT:", (error_text or "<none>")[:500])

        elements = await page.eval_on_selector_all(
            "input,button,a",
            "els => els.slice(0,30).map(e => ({tag:e.tagName, type:e.getAttribute('type'),"
            " name:e.getAttribute('name'), id:e.id,"
            " text:(e.innerText||'').trim().slice(0,40)}))",
        )
        print("FIRST_30_ELEMENTS:")
        for item in elements:
            print(" ", item)

        await page.screenshot(
            path=os.path.join(ROOT, "probe_after.png"), full_page=True
        )
        with open(os.path.join(ROOT, "probe_after.html"), "w", encoding="utf-8") as fh:
            fh.write(await page.content())

        await browser.close()
        return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
