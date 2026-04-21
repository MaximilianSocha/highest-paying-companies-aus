"""
etl.py — One-off ETL script
Reads remuneration CSV, resolves ABNs to company names via the ABN Lookup
free web service (GUID auth), and writes everything to a SQLite database.

Usage:
    pip install pandas requests
    python etl.py --csv data/remuneration.csv --guid YOUR_GUID_HERE --db backend/data.db

ABN Lookup GUID registration: https://abr.business.gov.au/Tools/WebServices
"""

import argparse
import sqlite3
import time
import xml.etree.ElementTree as ET
from pathlib import Path
import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ABN_LOOKUP_URL = (
    "https://abr.business.gov.au/abrxmlsearch/AbrXmlSearch.asmx/SearchByABNv202001"
)
REQUEST_DELAY_SECONDS = 0.2  # Be polite to the ABN web service
MAX_RETRIES = 3

# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS abn_cache (
    abn             TEXT PRIMARY KEY,
    company_name    TEXT,
    fetched_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS remuneration (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    abn                     TEXT NOT NULL,
    company_name            TEXT,
    reporting_period        TEXT,
    remuneration_quartile   TEXT,
    avg_remuneration        REAL,
    FOREIGN KEY (abn) REFERENCES abn_cache(abn)
);
"""


def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    conn.commit()
    print(f"[DB] Initialised database at {db_path}")
    return conn


# ---------------------------------------------------------------------------
# ABN Lookup
# ---------------------------------------------------------------------------


def fetch_company_name(abn: str, guid: str) -> str | None:
    """
    Call the ABN Lookup web service for a single ABN.
    Returns the entity name string, or None if not found / error.
    """
    params = {
        "searchString": abn,
        "includeHistoricalDetails": "N",
        "authenticationGuid": guid,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(ABN_LOOKUP_URL, params=params, timeout=10)
            response.raise_for_status()

            # Parse XML response
            root = ET.fromstring(response.text)
            ns = {"abr": "http://abr.business.gov.au/ABRXMLSearch/"}
            prefix = "abr:response/abr:businessEntity202001/"

            # Check for exception in response
            exception = root.find(prefix + "abr:exception", ns)
            if exception is not None:
                desc = exception.findtext(
                    "abn:exceptionDescription", default="", namespaces=ns
                )
                print(f"  [WARN] ABN {abn}: service exception — {desc}")
                return None

            # Try common possible name locations
            paths = [
                "abr:entityName",
                "abr:mainName/abr:organisationName",
                "abr:businessName/abr:organisationName",
            ]

            for path in paths:
                element = root.find(prefix + path, ns)
                if element is not None and element.text:
                    return element.text.strip()

            print(f"  [WARN] ABN {abn}: no name found in response")
            return None

        except requests.RequestException as e:
            print(
                f"  [WARN] ABN {abn}: request failed (attempt {attempt}/{MAX_RETRIES}) — {e}"
            )
            if attempt < MAX_RETRIES:
                time.sleep(2**attempt)  # Exponential backoff

    return None


def resolve_abns(
    abns: list[str], guid: str, conn: sqlite3.Connection
) -> dict[str, str | None]:
    """
    Resolve a list of ABNs to company names.
    - Checks the abn_cache table first to avoid redundant API calls.
    - Fetches only uncached ABNs from the web service.
    Returns a dict of {abn: company_name}.
    """
    # Load already-cached ABNs
    cached = dict(conn.execute("SELECT abn, company_name FROM abn_cache").fetchall())
    print(f"[ABN] {len(cached)} ABNs already cached in DB")

    to_fetch = [abn for abn in abns if abn not in cached]
    print(f"[ABN] {len(to_fetch)} ABNs to fetch from web service")

    results = dict(cached)

    for i, abn in enumerate(to_fetch, 1):
        print(f"  [{i}/{len(to_fetch)}] Fetching ABN {abn} ...", end=" ")
        name = fetch_company_name(abn, guid)
        print(name or "NOT FOUND")
        results[abn] = name

        # Cache in DB immediately (so progress is saved if script is interrupted)
        conn.execute(
            "INSERT OR REPLACE INTO abn_cache (abn, company_name) VALUES (?, ?)",
            (abn, name),
        )
        conn.commit()

        time.sleep(REQUEST_DELAY_SECONDS)

    return results


# ---------------------------------------------------------------------------
# ETL
# ---------------------------------------------------------------------------


def run_etl(csv_path: str, guid: str, db_path: str) -> None:
    # 1. Read CSV
    print(f"\n[CSV] Reading {csv_path} ...")
    df = pd.read_csv(csv_path, dtype={"ABN": str})

    # Normalise column names to lowercase with underscores
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    required = {
        "abn",
        "remuneration_quartile",
        "reporting_period",
        "avg_remuneration",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV is missing expected columns: {missing}")

    # Clean ABN: strip spaces, zero-pad to 11 digits
    df["abn"] = df["abn"].astype(str).str.replace(" ", "").str.strip().str.zfill(11)

    print(f"[CSV] {len(df):,} rows, {df['abn'].nunique():,} unique ABNs")

    # 2. Init DB
    conn = init_db(db_path)

    # 3. Resolve ABNs
    unique_abns = df["abn"].dropna().unique().tolist()
    abn_map = resolve_abns(unique_abns, guid, conn)

    # 4. Map company names onto dataframe
    df["company_name"] = df["abn"].map(abn_map)
    unresolved = df["company_name"].isna().sum()
    if unresolved:
        print(f"[INFO] Dropping {unresolved:,} rows with unresolved ABNs")
        df = df[df["company_name"].notna()]

    # 5. Write remuneration data
    print(f"\n[DB] Writing {len(df):,} rows to remuneration table ...")

    # Clear existing data so re-runs don't duplicate
    conn.execute("DELETE FROM remuneration")

    df[
        [
            "abn",
            "company_name",
            "reporting_period",
            "remuneration_quartile",
            "avg_remuneration",
        ]
    ].to_sql(
        "remuneration",
        conn,
        if_exists="append",
        index=False,
    )
    conn.commit()

    # 6. Summary
    count = conn.execute("SELECT COUNT(*) FROM remuneration").fetchone()[0]
    resolved = conn.execute(
        "SELECT COUNT(*) FROM remuneration WHERE company_name IS NOT NULL"
    ).fetchone()[0]
    print(f"\n[DONE] {count:,} rows written — {resolved:,} with company names resolved")
    print(f"[DONE] Database saved to {db_path}")
    conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ETL: remuneration CSV → SQLite with ABN lookup"
    )
    parser.add_argument("--csv", required=True, help="Path to remuneration CSV file")
    parser.add_argument(
        "--guid", required=True, help="Your ABN Lookup authentication GUID"
    )
    parser.add_argument(
        "--db",
        default="backend/data.db",
        help="Output SQLite database path (default: backend/data.db)",
    )
    args = parser.parse_args()

    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    run_etl(args.csv, args.guid, args.db)
