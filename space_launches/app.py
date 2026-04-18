import os
import json
import hashlib
from flask import Flask, jsonify, render_template, request
from datetime import datetime

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "data", "launches.json")
DETAILED_FILE = os.path.join(BASE_DIR, "data", "launches_detailed.json")
ROCKET_REF_FILE = os.path.join(BASE_DIR, "data", "rocket_reference.json")
FETCH_LOG_FILE = os.path.join(BASE_DIR, "fetch_log.jsonl")


def load_data():
    with open(DATA_FILE, "r") as f:
        return json.load(f)


def data_hash():
    with open(DATA_FILE, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


def file_mtime():
    return datetime.fromtimestamp(os.path.getmtime(DATA_FILE)).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/data")
def api_data():
    return jsonify(
        {
            "launches": load_data(),
            "hash": data_hash(),
            "updated_at": file_mtime(),
        }
    )


@app.route("/api/hash")
def api_hash():
    return jsonify({"hash": data_hash(), "updated_at": file_mtime()})


@app.route("/api/detailed")
def api_detailed():
    """Return per-launch records from launches_detailed.json, optionally filtered."""
    year = request.args.get("year")
    country = request.args.get("country")
    month = request.args.get("month")

    if not os.path.exists(DETAILED_FILE):
        return jsonify({"launches": [], "total": 0, "filters": {"year": year, "country": country, "month": month}})

    with open(DETAILED_FILE, "r") as f:
        data = json.load(f)

    launches = data if isinstance(data, list) else data.get("launches", [])

    if year:
        launches = [
            rec for rec in launches
            if str(rec.get("year", "")) == str(year)
            or (rec.get("date", "").startswith(str(year)))
        ]

    if month:
        launches = [
            rec for rec in launches
            if str(rec.get("month", "")) == str(month)
            or (len(rec.get("date", "")) >= 7 and rec["date"][5:7] == str(month).zfill(2))
        ]

    if country:
        country_lower = country.lower()
        launches = [
            rec for rec in launches
            if country_lower in str(rec.get("country", "")).lower()
        ]

    return jsonify({
        "launches": launches,
        "total": len(launches),
        "filters": {
            "year": year,
            "country": country,
            "month": month,
        },
    })


@app.route("/api/rockets")
def api_rockets():
    """Return rocket reference data from data/rocket_reference.json."""
    with open(ROCKET_REF_FILE, "r") as f:
        data = json.load(f)
    return jsonify(data)


@app.route("/api/prediction")
def api_prediction():
    """Return Q4 2026 prediction data from launches.json q4_2026_prediction field."""
    data = load_data()
    prediction = data.get("q4_2026_prediction", {})
    return jsonify(prediction)


@app.route("/api/status")
def api_status():
    """Return system status: file presence, last fetch timestamp, and per-year summary counts."""
    # Detailed file existence and size
    detailed_exists = os.path.exists(DETAILED_FILE)
    detailed_size_bytes = os.path.getsize(DETAILED_FILE) if detailed_exists else None

    # Last fetch timestamp from fetch_log.jsonl (last non-empty line)
    last_fetch = None
    if os.path.exists(FETCH_LOG_FILE):
        try:
            with open(FETCH_LOG_FILE, "r") as f:
                lines = [line.strip() for line in f if line.strip()]
            if lines:
                last_line = json.loads(lines[-1])
                last_fetch = last_line
        except (json.JSONDecodeError, OSError):
            last_fetch = None

    # Per-year summary counts from launches.json
    year_counts = {}
    try:
        data = load_data()
        launches_list = None
        if isinstance(data, list):
            launches_list = data
        elif isinstance(data, dict):
            # Try common keys
            for key in ("launches", "data", "records"):
                if key in data and isinstance(data[key], list):
                    launches_list = data[key]
                    break

        if launches_list is not None:
            for rec in launches_list:
                year = None
                if "year" in rec:
                    year = str(rec["year"])
                elif "date" in rec and len(str(rec["date"])) >= 4:
                    year = str(rec["date"])[:4]
                if year:
                    year_counts[year] = year_counts.get(year, 0) + 1
    except (OSError, json.JSONDecodeError, KeyError):
        pass

    return jsonify({
        "launches_detailed": {
            "exists": detailed_exists,
            "size_bytes": detailed_size_bytes,
        },
        "last_fetch": last_fetch,
        "year_counts": year_counts,
    })


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
