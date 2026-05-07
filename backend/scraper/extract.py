"""Extract dynamic table rows into list of dicts."""

from __future__ import annotations

import logging
from typing import Any

from playwright.async_api import Page

logger = logging.getLogger(__name__)


async def extract_table_rows(
    page: Page,
    table_selector: str,
    row_selector: str,
) -> list[dict[str, Any]]:
    """
    Parse the first matching table: first <tr> is treated as header row,
    remaining <tr> rows become dicts (dynamic columns).
    """
    # `row_selector` is reserved for portals that need scoped row locators.
    # We default to parsing the first matching <table> and its <tr> rows.
    _ = row_selector

    # Fast path: single JS evaluation in the page context (much faster than per-cell inner_text round trips).
    try:
        records = await page.eval_on_selector(
            table_selector,
            """(table) => {
              const rows = Array.from(table.querySelectorAll('tr'));
              if (rows.length < 2) return [];

              const headerCells = Array.from(rows[0].querySelectorAll('th,td'));
              const headers = headerCells.map((c, i) => {
                const t = (c && c.innerText ? c.innerText : '').trim();
                return t || `col_${i}`;
              });

              const out = [];
              for (let ri = 1; ri < rows.length; ri++) {
                const cells = Array.from(rows[ri].querySelectorAll('td,th'));
                if (!cells.length) continue;
                const obj = {};
                for (let ci = 0; ci < cells.length; ci++) {
                  const key = headers[ci] ?? `col_${ci}`;
                  const val = ((cells[ci] && cells[ci].innerText) ? cells[ci].innerText : '').trim();
                  obj[key] = val;
                }
                // Keep behavior aligned with the slow path: skip fully empty rows.
                if (Object.values(obj).some(v => v)) out.push(obj);
              }
              return out;
            }""",
        )
        if isinstance(records, list):
            return records
    except Exception:
        # Fallback to the slower locator-based extraction for portals with unusual DOM / shadow roots.
        pass

    table = page.locator(table_selector).first
    if await table.count() == 0:
        logger.warning("No table found for selector %s", table_selector)
        return []

    rows = table.locator("tr")
    n = await rows.count()
    if n < 2:
        return []

    header_cells = rows.nth(0).locator("th, td")
    hc = await header_cells.count()
    headers: list[str] = []
    for i in range(hc):
        text = (await header_cells.nth(i).inner_text()).strip()
        headers.append(text or f"col_{i}")

    records: list[dict[str, Any]] = []
    for ri in range(1, n):
        row = rows.nth(ri)
        cells = row.locator("td, th")
        cc = await cells.count()
        if cc == 0:
            continue
        row_dict: dict[str, Any] = {}
        for ci in range(cc):
            key = headers[ci] if ci < len(headers) else f"col_{ci}"
            val = (await cells.nth(ci).inner_text()).strip()
            row_dict[key] = val
        if any(v for v in row_dict.values()):
            records.append(row_dict)
    return records


async def click_next_if_present(page: Page, next_selector: str) -> bool:
    if not next_selector.strip():
        return False
    loc = page.locator(next_selector).first
    if await loc.count() == 0:
        return False
    try:
        if await loc.is_disabled():
            return False
    except Exception:
        pass
    try:
        await loc.click(timeout=5000)
        return True
    except Exception:
        return False
