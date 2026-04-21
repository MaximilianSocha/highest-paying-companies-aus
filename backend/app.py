"""
app.py — Flask API for remuneration data
Serves paginated, sortable remuneration data from SQLite.

Usage:
    python app.py

Endpoints:
    GET /api/remuneration
        Query params:
            sort_by   — column to sort by (default: company_name)
                        valid values: avg_all, lower, lower_mid, upper_mid, upper, reporting_period
            order     — asc | desc (default: asc)
            page      — page number, 1-indexed (default: 1)
            page_size — rows per page (default: 50)
            search    — optional company name filter (case-insensitive)
    GET /api/remuneration/count
        Returns total row count (respects search param)
"""

import sqlite3
from pathlib import Path
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

DB_PATH = Path(__file__).parent / "data.db"

# Map of API sort_by values → SQLite column/expression
SORT_COLUMNS = {
    "company_name": "company_name",
    "avg_all": "avg_all",
    "lower": "lower_q",
    "lower_mid": "lower_mid_q",
    "upper_mid": "upper_mid_q",
    "upper": "upper_q",
    "reporting_period": "reporting_period",
}


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.route("/")
def index():
    """Serve the HTML frontend"""
    return render_template("index.html")


def build_pivoted_query(
    search: str | None, sort_col: str, order: str, limit: int, offset: int
) -> tuple[str, list]:
    """
    The raw data has one row per (abn, quartile, reporting_period).
    This query pivots quartiles into columns so each company/period is one row.
    """
    where_clause = "WHERE company_name IS NOT NULL"
    params: list = []

    if search:
        where_clause += " AND LOWER(company_name) LIKE LOWER(?)"
        params.append(f"%{search}%")

    # Validate sort + order to prevent SQL injection
    if sort_col not in SORT_COLUMNS.values():
        sort_col = "company_name"
    if order not in ("asc", "desc"):
        order = "asc"

    sql = f"""
        SELECT
            company_name,
            MAX(reporting_period) AS reporting_period,
            MAX(CASE WHEN remuneration_quartile = 'Lower quartile'           THEN avg_remuneration END) AS lower_q,
            MAX(CASE WHEN remuneration_quartile = 'Lower middle quartile'    THEN avg_remuneration END) AS lower_mid_q,
            MAX(CASE WHEN remuneration_quartile = 'Upper middle quartile'    THEN avg_remuneration END) AS upper_mid_q,
            MAX(CASE WHEN remuneration_quartile = 'Upper quartile'           THEN avg_remuneration END) AS upper_q,
            MAX(CASE WHEN remuneration_quartile = 'Total workforce'          THEN avg_remuneration END) AS avg_all
        FROM remuneration
        {where_clause}
        GROUP BY company_name
        ORDER BY {sort_col} {order} NULLS LAST
        LIMIT ? OFFSET ?
    """
    params += [limit, offset]
    return sql, params


def build_count_query(search: str | None) -> tuple[str, list]:
    where_clause = "WHERE company_name IS NOT NULL"
    params: list = []
    if search:
        where_clause += " AND LOWER(company_name) LIKE LOWER(?)"
        params.append(f"%{search}%")

    sql = f"""
        SELECT COUNT(DISTINCT company_name || '|' || reporting_period)
        FROM remuneration
        {where_clause}
    """
    return sql, params


@app.route("/api/remuneration")
def get_remuneration():
    sort_by = request.args.get("sort_by", "company_name")
    order = request.args.get("order", "asc").lower()
    page = max(1, int(request.args.get("page", 1)))
    page_size = min(200, max(1, int(request.args.get("page_size", 50))))
    search = request.args.get("search", "").strip() or None

    sort_col = SORT_COLUMNS.get(sort_by, "company_name")
    offset = (page - 1) * page_size

    conn = get_db()
    try:
        sql, params = build_pivoted_query(search, sort_col, order, page_size, offset)
        rows = conn.execute(sql, params).fetchall()

        data = [
            {
                "company_name": row["company_name"],
                "reporting_period": row["reporting_period"],
                "avg_all": row["avg_all"],
                "lower": row["lower_q"],
                "lower_mid": row["lower_mid_q"],
                "upper_mid": row["upper_mid_q"],
                "upper": row["upper_q"],
            }
            for row in rows
        ]
        return jsonify({"data": data, "page": page, "page_size": page_size})
    finally:
        conn.close()


@app.route("/api/remuneration/count")
def get_count():
    search = request.args.get("search", "").strip() or None
    conn = get_db()
    try:
        sql, params = build_count_query(search)
        total = conn.execute(sql, params).fetchone()[0]
        return jsonify({"total": total})
    finally:
        conn.close()


if __name__ == "__main__":
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found at {DB_PATH}. Run etl.py first.")
    print(f"[Flask] Using database: {DB_PATH}")
    app.run(debug=True, port=5000)
