import requests
import csv
import time
import json
import os

# ==================================================
# USER CONFIG
# ==================================================

API_KEY = "REMOVED_API_KEY"

BASE_URL = "https://api.clarivate.com/apis/wos-starter/v2/documents"

QUERY = '(TI="Continual Learning" OR TI="Lifelong Learning" OR TI="Incremental Learning") AND (TS=EHR OR TS=MRI OR TS=Oncology)'

# normalize whitespace (IMPORTANT for WoS parser)
# QUERY = " ".join(QUERY.split())

DB = "WOS"
LIMIT = 50
SLEEP_SECONDS = 1.5

OUTPUT_FILE = "wos_results_v2.csv"
CHECKPOINT_FILE = "progress.json"


# ==================================================
# CHECKPOINT HANDLING
# ==================================================


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {"page": 1, "written": 0}


def save_checkpoint(page, written):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"page": page, "written": written}, f)


# ==================================================
# METADATA PARSER
# ==================================================


def parse_record(rec):

    out = {}

    # ---------------- UID ----------------
    out["uid"] = rec.get("uid")

    # ---------------- TITLE ----------------
    out["title"] = rec.get("title")

    # ---------------- AUTHORS ----------------
    authors = ""
    names_block = rec.get("names", {})
    if isinstance(names_block, dict):
        auth_list = names_block.get("authors", [])
        if isinstance(auth_list, list):
            authors = "; ".join(a.get("displayName", "") for a in auth_list)

    out["authors"] = authors

    # ---------------- SOURCE INFO ----------------
    source_block = rec.get("source", {})
    out["source_title"] = source_block.get("sourceTitle")
    out["publish_year"] = source_block.get("publishYear")
    out["publish_month"] = source_block.get("publishMonth")
    out["volume"] = source_block.get("volume")
    out["issue"] = source_block.get("issue")

    # pages count or range
    pages_block = source_block.get("pages", {})
    if isinstance(pages_block, dict):
        out["pages"] = pages_block.get("range") or pages_block.get("count")
    else:
        out["pages"] = None

    # ---------------- IDENTIFIERS (⭐ DOI FIX) ----------------
    identifiers = rec.get("identifiers", {})

    out["doi"] = identifiers.get("doi")
    out["issn"] = identifiers.get("issn")
    out["eissn"] = identifiers.get("eissn")
    out["pmid"] = identifiers.get("pmid")

    # ---------------- KEYWORDS ----------------
    kw_block = rec.get("keywords", {})
    if isinstance(kw_block, dict):
        kw_list = kw_block.get("authorKeywords", [])
        out["keywords"] = "; ".join(kw_list)
    else:
        out["keywords"] = ""

    # ---------------- WOS LINK ----------------
    links = rec.get("links", {})
    out["wos_url"] = links.get("record")

    return out


# ==================================================
# CSV WRITER
# ==================================================


def append_to_csv(rows):

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
# API SETUP
# ==================================================

headers = {"X-ApiKey": API_KEY, "Accept": "application/json"}

checkpoint = load_checkpoint()

params = {
    "db": DB,
    "q": QUERY,
    "limit": LIMIT,
    "page": checkpoint["page"],
    "detail": "full",
    "sortField": "LD+D",
}

written_total = checkpoint["written"]
total_records = None

print(f"Resuming from page {params['page']} | Records already written: {written_total}")

# show encoded URL once for debugging
print("Encoded URL:", requests.Request("GET", BASE_URL, params=params).prepare().url)


# ==================================================
# MAIN LOOP
# ==================================================

while True:

    print(f"Fetching page {params['page']}...")

    try:
        r = requests.get(BASE_URL, headers=headers, params=params, timeout=60)
    except Exception as e:
        print("Connection error:", e)
        break

    if r.status_code != 200:
        print("API error:", r.status_code, r.text)
        print("Stopping safely — resume later.")
        break

    data = r.json()

    # ---------- HYBRID v2 FIX ----------
    meta = data.get("metadata", {})

    if total_records is None:
        total_records = meta.get("totalRecords") or meta.get("total") or 0
        print("Total records available:", total_records)

    # some v2 clusters return 'data', others 'hits'
    records = data.get("data") or data.get("hits") or []
    # -----------------------------------

    if not records:
        print("No more records returned.")
        break

    rows = [parse_record(rec) for rec in records]

    append_to_csv(rows)

    written_total += len(rows)
    save_checkpoint(params["page"], written_total)

    print(f"Written so far: {written_total}")

    params["page"] += 1

    if written_total >= total_records:
        print("All available records downloaded.")
        break

    time.sleep(SLEEP_SECONDS)

print("Finished harvesting.")
