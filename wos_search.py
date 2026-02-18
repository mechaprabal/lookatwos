import requests
import csv
import time
import json
import os
import math
import random
from typing import Dict, List, Any, Set

# ==================================================
# USER CONFIG
# ==================================================

API_KEY = "REMOVED_API_KEY"  # <-- use a valid key
BASE_URL = "https://api.clarivate.com/apis/wos-starter/v2/documents"

QUERY = 'TI=("Continual Learning") AND PY=2021-2026 AND DT=(Article OR Review)'
QUERY = " ".join(QUERY.split())  # normalize whitespace

DB = "WOS"
LIMIT = 50  # Starter max
BASE_SLEEP = 1.5  # base delay between successful calls
MAX_RETRIES = 6  # retries per page
REQUEST_TIMEOUT = 60  # seconds

OUTPUT_FILE = "wos_results_v2.csv"
CHECKPOINT_FILE = "progress.json"
UID_CACHE_FILE = "uid_cache.json"  # optional de-duplication across restarts


# ==================================================
# CHECKPOINT & UID CACHE
# ==================================================


def load_checkpoint() -> Dict[str, int]:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {"page": 1, "written": 0}


def save_checkpoint(page: int, written: int):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"page": page, "written": written}, f)


def load_uid_cache() -> Set[str]:
    if os.path.exists(UID_CACHE_FILE):
        with open(UID_CACHE_FILE, "r") as f:
            return set(json.load(f))
    return set()


def save_uid_cache(uid_set: Set[str]):
    with open(UID_CACHE_FILE, "w") as f:
        json.dump(sorted(list(uid_set)), f)


# ==================================================
# METADATA PARSER (Starter v2 schema)
# ==================================================


def parse_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    out = {}

    out["uid"] = rec.get("uid")
    out["title"] = rec.get("title")

    # authors
    authors = ""
    names_block = rec.get("names", {})
    if isinstance(names_block, dict):
        auth_list = names_block.get("authors", [])
        if isinstance(auth_list, list):
            authors = "; ".join(a.get("displayName", "") for a in auth_list)
    out["authors"] = authors

    # source info
    source_block = rec.get("source", {})
    out["source_title"] = source_block.get("sourceTitle")
    out["publish_year"] = source_block.get("publishYear")
    out["publish_month"] = source_block.get("publishMonth")
    out["volume"] = source_block.get("volume")
    out["issue"] = source_block.get("issue")

    pages_block = source_block.get("pages", {})
    if isinstance(pages_block, dict):
        out["pages"] = pages_block.get("range") or pages_block.get("count")
    else:
        out["pages"] = None

    # identifiers
    identifiers = rec.get("identifiers", {})
    out["doi"] = identifiers.get("doi")
    out["issn"] = identifiers.get("issn")
    out["eissn"] = identifiers.get("eissn")
    out["pmid"] = identifiers.get("pmid")

    # keywords
    kw_block = rec.get("keywords", {})
    if isinstance(kw_block, dict):
        kw_list = kw_block.get("authorKeywords", [])
        out["keywords"] = "; ".join(kw_list)
    else:
        out["keywords"] = ""

    # record link
    links = rec.get("links", {})
    out["wos_url"] = links.get("record")

    return out


# ==================================================
# CSV WRITER
# ==================================================


def append_to_csv(rows: List[Dict[str, Any]]):
    if not rows:
        return
    file_exists = os.path.exists(OUTPUT_FILE)
    keys = rows[0].keys()

    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


# ==================================================
# HTTP WITH RETRY + THROTTLING
# ==================================================


def request_with_retry(
    session: requests.Session, url: str, headers: Dict[str, str], params: Dict[str, Any]
) -> requests.Response:
    """
    Robust GET with exponential backoff + jitter.
    Retries on 429 and transient 5xx errors.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(
                url, headers=headers, params=params, timeout=REQUEST_TIMEOUT
            )
        except requests.exceptions.RequestException as e:
            # network error → backoff and retry
            sleep = min(60, (2**attempt)) + random.uniform(0, 0.5)
            print(
                f"[WARN] Network error: {e} | retrying in {sleep:.2f}s (attempt {attempt}/{MAX_RETRIES})"
            )
            time.sleep(sleep)
            continue

        # success
        if resp.status_code == 200:
            return resp

        # rate limited
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            sleep = (
                float(retry_after)
                if retry_after
                else min(60, (2**attempt)) + random.uniform(0, 0.5)
            )
            print(
                f"[WARN] 429 Rate limited | sleeping {sleep:.2f}s (attempt {attempt}/{MAX_RETRIES})"
            )
            time.sleep(sleep)
            continue

        # transient server errors
        if resp.status_code >= 500:
            sleep = min(60, (2**attempt)) + random.uniform(0, 0.5)
            print(
                f"[WARN] Server {resp.status_code} | retrying in {sleep:.2f}s (attempt {attempt}/{MAX_RETRIES})"
            )
            time.sleep(sleep)
            continue

        # hard failure (4xx except 429)
        print(f"[ERROR] HTTP {resp.status_code}: {resp.text}")
        resp.raise_for_status()

    raise RuntimeError("Max retries exceeded for request")


# ==================================================
# MAIN HARVEST LOOP
# ==================================================


def main():

    headers = {"X-ApiKey": API_KEY.strip(), "Accept": "application/json"}

    checkpoint = load_checkpoint()
    seen_uids = load_uid_cache()

    params = {
        "db": DB,
        "q": QUERY,
        "limit": LIMIT,
        "page": checkpoint["page"],
        "detail": "full",
        "sortField": "LD+D",
    }

    written_total = checkpoint["written"]
    total_pages = None

    print(
        f"Resuming from page {params['page']} | Records already written: {written_total}"
    )
    print(
        "Encoded URL:", requests.Request("GET", BASE_URL, params=params).prepare().url
    )

    session = requests.Session()

    while True:
        print(f"\n[INFO] Fetching page {params['page']}...")

        resp = request_with_retry(session, BASE_URL, headers, params)
        data = resp.json()

        meta = data.get("metadata", {})
        if total_pages is None:
            total_records = meta.get("totalRecords") or meta.get("total") or 0
            limit_returned = meta.get("limit", LIMIT)
            total_pages = (
                math.ceil(total_records / limit_returned) if limit_returned else 0
            )
            print(
                f"[INFO] Total records: {total_records} | limit: {limit_returned} | total pages: {total_pages}"
            )

        # hybrid v2: 'data' or 'hits'
        records = data.get("data") or data.get("hits") or []
        if not records:
            print("[INFO] No records returned; stopping.")
            break

        # parse + optional de-duplication
        rows = []
        new_count = 0
        for rec in records:
            uid = rec.get("uid")
            if uid and uid in seen_uids:
                continue
            row = parse_record(rec)
            rows.append(row)
            if uid:
                seen_uids.add(uid)
                new_count += 1

        append_to_csv(rows)

        written_total += len(rows)
        save_checkpoint(params["page"], written_total)
        save_uid_cache(seen_uids)

        print(
            f"[INFO] Page {params['page']} done | new rows: {new_count} | total written: {written_total}"
        )

        params["page"] += 1

        # completion condition
        if total_pages and params["page"] > total_pages:
            print("[INFO] All pages downloaded.")
            break

        # polite throttle
        time.sleep(BASE_SLEEP + random.uniform(0.0, 0.3))

    print("\n[INFO] Harvest finished.")


if __name__ == "__main__":
    main()
