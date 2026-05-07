"""Standalone Playwright probe for the portal login page.

Run on the deploy server:
    cd /root/Amity_Primary && TZ=Asia/Kolkata \
        ./.venv/bin/python scripts/probe_login.py

Outputs probe.png and probe.html in the project root and prints what
Chromium actually sees (status, final URL, list of input/button elements).
"""
from __future__ import annotations

import asyncio
import os
import sys

from playwright.async_api import async_playwright

URL = os.environ.get("PORTAL_LOGIN_URL", "https://portal.amity.edu/Lead")
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


async def main() -> int:
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
            return 2

        print("HTTP_STATUS:", response.status if response else None)
        print("FINAL_URL  :", page.url)

        screenshot_path = os.path.join(ROOT, "probe.png")
        html_path = os.path.join(ROOT, "probe.html")
        await page.screenshot(path=screenshot_path, full_page=True)
        html = await page.content()
        with open(html_path, "w", encoding="utf-8") as fh:
            fh.write(html)

        elements = await page.eval_on_selector_all(
            "input,button",
            "els => els.map(e => ({tag:e.tagName, type:e.getAttribute('type'),"
            " name:e.getAttribute('name'), id:e.id,"
            " placeholder:e.getAttribute('placeholder'),"
            " text:(e.innerText||'').slice(0,40)}))",
        )
        print("INPUTS_FOUND:", len(elements))
        for item in elements[:30]:
            print(" ", item)

        await browser.close()
        return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
