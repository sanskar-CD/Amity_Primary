"""Dedupe keys: phone, email, or stable hash of row content."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any


def _clean_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_mobile(raw: str) -> str:
    digits = re.sub(r"\D", "", _clean_str(raw))
    if len(digits) >= 10:
        return digits[-10:]
    return digits


def normalize_email(raw: str) -> str:
    return _clean_str(raw).lower()


def detect_mobile_column(fieldnames: list[str]) -> str | None:
    patterns = (
        r"mobile",
        r"phone",
        r"contact",
        r"cell",
        r"whatsapp",
        r"tel",
    )
    for name in fieldnames:
        lower = name.lower()
        for pat in patterns:
            if re.search(pat, lower):
                return name
    return None


def detect_email_column(fieldnames: list[str]) -> str | None:
    for name in fieldnames:
        if "email" in name.lower() or "e-mail" in name.lower():
            return name
    return None


def compute_dedupe_key(record: dict[str, Any]) -> str:
    if not isinstance(record, dict):
        record = {"_raw": str(record)}
    keys = [str(k) for k in record.keys()]
    mobile_col = detect_mobile_column(keys)
    if mobile_col:
        m = normalize_mobile(record.get(mobile_col, ""))
        if len(m) >= 8:
            return f"phone:{m}"
    email_col = detect_email_column(keys)
    if email_col:
        e = normalize_email(record.get(email_col, ""))
        if e and "@" in e:
            return f"email:{e}"
    payload = json.dumps(record, sort_keys=True, ensure_ascii=False, default=str)
    return "hash:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()
