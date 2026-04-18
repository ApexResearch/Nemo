#!/usr/bin/env python3
"""
autoresearch_fetcher.py
Fetches real orbital launch data from Wikipedia "in spaceflight" pages and
supplementary RSS/web sources, normalizes it, and writes structured JSON files
for the Flask launches dashboard.

Usage:
    python autoresearch_fetcher.py [--years 2023 2024 2025 2026] [--dry-run] [-v]

Dependencies: requests, beautifulsoup4, lxml
"""

import argparse
import json
import os
import re
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Optional
from xml.etree import ElementTree as ET

try:
    import requests
    from bs4 import BeautifulSoup, Tag
except ImportError as e:
    sys.exit(
        f"Missing dependency: {e}. Install with: pip install requests beautifulsoup4 lxml"
    )

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
LAUNCHES_JSON = os.path.join(DATA_DIR, "launches.json")
LAUNCHES_DETAILED_JSON = os.path.join(DATA_DIR, "launches_detailed.json")
FETCH_LOG_JSONL = os.path.join(DATA_DIR, "fetch_log.jsonl")

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
VERBOSE = False


def vlog(msg: str) -> None:
    if VERBOSE:
        print(f"  [v] {msg}", file=sys.stderr)


def log(msg: str) -> None:
    print(f"[*] {msg}", file=sys.stderr)


def warn(msg: str) -> None:
    print(f"[!] {msg}", file=sys.stderr)


def write_fetch_log(source: str, status: str, count: int, extra: Optional[str] = None) -> None:
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "status": status,
        "count": count,
    }
    if extra:
        entry["note"] = extra
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(FETCH_LOG_JSONL, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")
    except Exception as exc:
        warn(f"Could not write fetch log: {exc}")


# ---------------------------------------------------------------------------
# Network helpers with retry
# ---------------------------------------------------------------------------
SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (compatible; SpaceLaunchResearcher/1.0; "
            "+https://github.com/space-dashboard)"
        )
    }
)


def fetch_url(url: str, max_retries: int = 3, delay: float = 2.0) -> Optional[requests.Response]:
    for attempt in range(max_retries):
        try:
            vlog(f"GET {url} (attempt {attempt+1})")
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            warn(f"Request failed [{attempt+1}/{max_retries}]: {exc}")
            if attempt < max_retries - 1:
                time.sleep(delay)
    return None


# ---------------------------------------------------------------------------
# Rocket normalization catalogue
# ---------------------------------------------------------------------------
# Each entry: (pattern_re, rocket_name, family, country, operator)
ROCKET_PATTERNS: list[tuple[str, str, str, str, str]] = [
    # SpaceX
    (r"falcon\s*9", "Falcon 9", "Falcon 9", "USA", "SpaceX"),
    (r"falcon\s*heavy", "Falcon Heavy", "Falcon Heavy", "USA", "SpaceX"),
    (r"starship", "Starship", "Starship", "USA", "SpaceX"),
    # RocketLab
    (r"electron", "Electron", "Electron", "New Zealand", "RocketLab"),
    # ULA / USA
    (r"vulcan\s*centaur", "Vulcan Centaur", "Vulcan", "USA", "ULA"),
    (r"atlas\s*v", "Atlas V", "Atlas V", "USA", "ULA"),
    (r"delta\s*iv\s*heavy", "Delta IV Heavy", "Delta IV", "USA", "ULA"),
    (r"delta\s*iv", "Delta IV", "Delta IV", "USA", "ULA"),
    # Blue Origin
    (r"new\s*glenn", "New Glenn", "New Glenn", "USA", "Blue Origin"),
    (r"new\s*shepard", "New Shepard", "New Shepard", "USA", "Blue Origin"),
    # Northrop / Orbital
    (r"antares", "Antares", "Antares", "USA", "Northrop Grumman"),
    (r"minotaur", "Minotaur", "Minotaur", "USA", "Northrop Grumman"),
    # Firefly
    (r"firefly\s*alpha", "Firefly Alpha", "Alpha", "USA", "Firefly Aerospace"),
    # ABL
    (r"abl\s*rs1", "ABL RS1", "RS1", "USA", "ABL Space Systems"),
    # Relativity
    (r"terran\s*1", "Terran 1", "Terran", "USA", "Relativity Space"),
    (r"terran\s*r", "Terran R", "Terran R", "USA", "Relativity Space"),
    # China — Long March
    (r"long\s*march\s*11|cz[-\s]?11", "CZ-11", "Long March 11", "China", "CASC"),
    (r"long\s*march\s*2[cC]|cz[-\s]?2[cC]", "CZ-2C", "Long March 2", "China", "CASC"),
    (r"long\s*march\s*2[dD]|cz[-\s]?2[dD]", "CZ-2D", "Long March 2", "China", "CASC"),
    (r"long\s*march\s*2[fF]|cz[-\s]?2[fF]", "CZ-2F", "Long March 2", "China", "CASC"),
    (r"long\s*march\s*3[bB]|cz[-\s]?3[bB]", "CZ-3B", "Long March 3", "China", "CASC"),
    (r"long\s*march\s*4[bB]|cz[-\s]?4[bB]", "CZ-4B", "Long March 4", "China", "CASC"),
    (r"long\s*march\s*4[cC]|cz[-\s]?4[cC]", "CZ-4C", "Long March 4", "China", "CASC"),
    (r"long\s*march\s*5[bB]|cz[-\s]?5[bB]", "CZ-5B", "Long March 5", "China", "CASC"),
    (r"long\s*march\s*5|cz[-\s]?5\b", "CZ-5", "Long March 5", "China", "CASC"),
    (r"long\s*march\s*6[aA]|cz[-\s]?6[aA]", "CZ-6A", "Long March 6", "China", "CASC"),
    (r"long\s*march\s*6|cz[-\s]?6\b", "CZ-6", "Long March 6", "China", "CASC"),
    (r"long\s*march\s*7[aA]|cz[-\s]?7[aA]", "CZ-7A", "Long March 7", "China", "CASC"),
    (r"long\s*march\s*7|cz[-\s]?7\b", "CZ-7", "Long March 7", "China", "CASC"),
    (r"long\s*march\s*8|cz[-\s]?8", "CZ-8", "Long March 8", "China", "CASC"),
    # China — commercial
    (r"kz[-\s]?1[aA]|kuaizhou[-\s]?1[aA]", "KZ-1A", "Kuaizhou", "China", "CASIC"),
    (r"kuaizhou", "Kuaizhou", "Kuaizhou", "China", "CASIC"),
    (r"ceres[-\s]?1", "Ceres-1", "Ceres", "China", "Galactic Energy"),
    (r"zhuque[-\s]?2|tianlong|zq[-\s]?2", "Zhuque-2", "Zhuque", "China", "LandSpace"),
    (r"zhuque[-\s]?3|zq[-\s]?3", "Zhuque-3", "Zhuque", "China", "LandSpace"),
    (r"hyperbola[-\s]?1", "Hyperbola-1", "Hyperbola", "China", "i-Space"),
    (r"jielong[-\s]?3", "Jielong-3", "Jielong", "China", "CASC"),
    (r"jielong", "Jielong", "Jielong", "China", "CASC"),
    (r"lijian[-\s]?1", "Lijian-1", "Lijian", "China", "CASIC"),
    (r"gravity[-\s]?1", "Gravity-1", "Gravity", "China", "Orienspace"),
    (r"tianlong[-\s]?3", "Tianlong-3", "Tianlong", "China", "Space Pioneer"),
    (r"tianlong[-\s]?2", "Tianlong-2", "Tianlong", "China", "Space Pioneer"),
    (r"tianlong", "Tianlong", "Tianlong", "China", "Space Pioneer"),
    (r"smart\s*dragon|sd[-\s]?1|smart[-\s]?dragon", "Smart Dragon", "Smart Dragon", "China", "CASC"),
    # Russia
    (r"soyuz[-\s]?2\.1[bB]", "Soyuz-2.1b", "Soyuz-2", "Russia", "Roscosmos"),
    (r"soyuz[-\s]?2\.1[aA]", "Soyuz-2.1a", "Soyuz-2", "Russia", "Roscosmos"),
    (r"soyuz[-\s]?2", "Soyuz-2", "Soyuz-2", "Russia", "Roscosmos"),
    (r"soyuz", "Soyuz", "Soyuz", "Russia", "Roscosmos"),
    (r"proton[-\s]?m", "Proton-M", "Proton", "Russia", "Roscosmos"),
    (r"proton", "Proton", "Proton", "Russia", "Roscosmos"),
    (r"angara[-\s]?a5", "Angara-A5", "Angara", "Russia", "Roscosmos"),
    (r"angara[-\s]?1\.2", "Angara-1.2", "Angara", "Russia", "Roscosmos"),
    (r"angara", "Angara", "Angara", "Russia", "Roscosmos"),
    (r"rokot", "Rokot", "Rokot", "Russia", "Roscosmos"),
    # Europe
    (r"ariane\s*6", "Ariane 6", "Ariane 6", "Europe", "ArianeGroup"),
    (r"ariane\s*5", "Ariane 5", "Ariane 5", "Europe", "ArianeGroup"),
    (r"vega[-\s]?c", "Vega-C", "Vega", "Europe", "Avio"),
    (r"vega", "Vega", "Vega", "Europe", "Avio"),
    # India
    (r"lvm3|lvm[-\s]?3", "LVM3", "LVM3", "India", "ISRO"),
    (r"gslv\s*mk\s*iii|gslv3", "LVM3", "LVM3", "India", "ISRO"),
    (r"gslv", "GSLV", "GSLV", "India", "ISRO"),
    (r"pslv[-\s]?xl", "PSLV-XL", "PSLV", "India", "ISRO"),
    (r"pslv[-\s]?ca", "PSLV-CA", "PSLV", "India", "ISRO"),
    (r"pslv", "PSLV", "PSLV", "India", "ISRO"),
    (r"sslv", "SSLV", "SSLV", "India", "ISRO"),
    # Japan
    (r"h3", "H3", "H3", "Japan", "JAXA"),
    (r"h-iia|h2a", "H-IIA", "H-IIA", "Japan", "JAXA"),
    (r"h-iib|h2b", "H-IIB", "H-IIB", "Japan", "JAXA"),
    (r"epsilon", "Epsilon", "Epsilon", "Japan", "JAXA"),
    # South Korea
    (r"nuri|kslv[-\s]?ii", "Nuri", "Nuri", "South Korea", "KARI"),
    # Iran
    (r"qaem[-\s]?100|qased", "Qaem-100", "Qaem", "Iran", "IRGC"),
    (r"safir", "Safir", "Safir", "Iran", "ISA"),
    (r"simorgh", "Simorgh", "Simorgh", "Iran", "ISA"),
    # DPRK
    (r"chollima|paektusan|hwasong", "Chollima-1", "Chollima", "DPRK", "NADA"),
    (r"malligyong", "Malligyong", "Malligyong", "DPRK", "NADA"),
    # Israel
    (r"shavit", "Shavit", "Shavit", "Israel", "IAI"),
]

# Compile patterns once
_COMPILED_ROCKETS: list[tuple[re.Pattern, str, str, str, str]] = [
    (re.compile(pat, re.IGNORECASE), name, family, country, operator)
    for pat, name, family, country, operator in ROCKET_PATTERNS
]

# ---------------------------------------------------------------------------
# Orbit normalization
# ---------------------------------------------------------------------------
ORBIT_PATTERNS: list[tuple[str, str]] = [
    (r"\bsso\b|sun.?synchronous", "SSO"),
    (r"\bleo\b|low\s*earth", "LEO"),
    (r"\bgto\b|geosynchronous\s*transfer|geostationary\s*transfer", "GTO"),
    (r"\bgeo\b|geostationary\b(?!\s*transfer)", "GEO"),
    (r"\bmeo\b|medium\s*earth", "MEO"),
    (r"\bheo\b|highly\s*elliptical|molniya", "HEO"),
    (r"\btli\b|trans[-\s]lunar|lunar\s*transfer", "TLI"),
    (r"\bpolar\b", "Polar"),
    (r"\bise\b|\bisp\b|deep\s*space|heliocentric|interplanetary", "Deep Space"),
    (r"\bhco\b|highly\s*circular", "HCO"),
    (r"\bsuborbital\b", "Suborbital"),
]
_COMPILED_ORBITS: list[tuple[re.Pattern, str]] = [
    (re.compile(pat, re.IGNORECASE), orb) for pat, orb in ORBIT_PATTERNS
]

# ---------------------------------------------------------------------------
# Payload type normalization
# ---------------------------------------------------------------------------
PAYLOAD_TYPE_PATTERNS: list[tuple[str, str]] = [
    (r"starlink|oneweb|telesat|globalstar|iridium|o3b|viasat|ses[-\s]|intelsat|eutelsat|amazon\s*kuiper|kuiper|broadband|communication", "Communications"),
    (r"earth\s*obs|eo[-\s]|remote\s*sens|imaging|imagery|optical\s*sat|sar\s*sat|sentinel|landsat|worldview|pleiades|spot\b|observation", "Earth Observation"),
    (r"nav|gnss|gps\s*(iii|block)|beidou|galileo|glonass|compass\s*nav", "Navigation"),
    (r"crewed|crew|astronaut|cosmonaut|taikonaut|dragon\s*(crew|capsule)\b|soyuz\s*ms|tianzhou(?!\s*cargo)|shenzhou|iss\s*crew|human\s*spaceflight", "Crewed"),
    (r"cargo|cygnus|dragon\s*cargo|progress|tianzhou|htvx|htv[-\s]\d|supply\s*mission|resupply", "Cargo"),
    (r"science|scientific|telescope|observatory|probe|lander|rover|sample\s*return|deep\s*space|lunar|mars|asteroid|comet|jwst|hubble|chandra", "Science"),
    (r"tech\s*demo|technology\s*demo|demonstrat|testbed|experimental|prototype|in-space\s*manufacturing", "Technology Demo"),
    (r"recon|reconnaissance|spy|national\s*security|nro\s*|classified|intelligence|sigint|imint|usa[-\s]\d", "National Security"),
    (r"rideshare|smallsat|cubesat|nanosatellite|picosat|microsatellite|transporter|bandwagon|multi.payload|multi.satellite", "Rideshare"),
]
_COMPILED_PAYLOAD_TYPES: list[tuple[re.Pattern, str]] = [
    (re.compile(pat, re.IGNORECASE), pt) for pat, pt in PAYLOAD_TYPE_PATTERNS
]

# ---------------------------------------------------------------------------
# Launch site normalization
# ---------------------------------------------------------------------------
LAUNCH_SITE_MAP: dict[str, tuple[str, str, str]] = {
    # key fragment -> (normalized_site, location, country)
    "slc-40": ("SLC-40", "Cape Canaveral SFS, Florida, USA", "USA"),
    "slc40": ("SLC-40", "Cape Canaveral SFS, Florida, USA", "USA"),
    "lc-39a": ("LC-39A", "Kennedy Space Center, Florida, USA", "USA"),
    "lc39a": ("LC-39A", "Kennedy Space Center, Florida, USA", "USA"),
    "slc-4e": ("SLC-4E", "Vandenberg SFB, California, USA", "USA"),
    "slc4e": ("SLC-4E", "Vandenberg SFB, California, USA", "USA"),
    "slc-6": ("SLC-6", "Vandenberg SFB, California, USA", "USA"),
    "cape canaveral": ("Cape Canaveral SFS", "Cape Canaveral SFS, Florida, USA", "USA"),
    "kennedy": ("LC-39A", "Kennedy Space Center, Florida, USA", "USA"),
    "vandenberg": ("Vandenberg SFB", "Vandenberg SFB, California, USA", "USA"),
    "wallops": ("Wallops Island", "Wallops Island, Virginia, USA", "USA"),
    "ksc": ("KSC", "Kennedy Space Center, Florida, USA", "USA"),
    "ccsfs": ("Cape Canaveral SFS", "Cape Canaveral SFS, Florida, USA", "USA"),
    "vsfb": ("Vandenberg SFB", "Vandenberg SFB, California, USA", "USA"),
    "boca chica": ("Starbase", "Boca Chica, Texas, USA", "USA"),
    "starbase": ("Starbase", "Boca Chica, Texas, USA", "USA"),
    "mahia": ("Launch Complex 1", "Mahia Peninsula, New Zealand", "New Zealand"),
    "lc1": ("Launch Complex 1", "Mahia Peninsula, New Zealand", "New Zealand"),
    "jiuquan": ("JSLC", "Jiuquan Satellite Launch Center, China", "China"),
    "jslc": ("JSLC", "Jiuquan Satellite Launch Center, China", "China"),
    "xichang": ("XSLC", "Xichang Satellite Launch Center, China", "China"),
    "xslc": ("XSLC", "Xichang Satellite Launch Center, China", "China"),
    "taiyuan": ("TSLC", "Taiyuan Satellite Launch Center, China", "China"),
    "tslc": ("TSLC", "Taiyuan Satellite Launch Center, China", "China"),
    "wenchang": ("WSLC", "Wenchang Space Launch Center, China", "China"),
    "wslc": ("WSLC", "Wenchang Space Launch Center, China", "China"),
    "baikonur": ("Baikonur", "Baikonur Cosmodrome, Kazakhstan", "Russia"),
    "plesetsk": ("Plesetsk", "Plesetsk Cosmodrome, Russia", "Russia"),
    "vostochny": ("Vostochny", "Vostochny Cosmodrome, Russia", "Russia"),
    "kourou": ("ELA", "Guiana Space Centre, French Guiana, Europe", "Europe"),
    "guiana": ("ELA", "Guiana Space Centre, French Guiana, Europe", "Europe"),
    "sriharikota": ("SDSC-SHAR", "Satish Dhawan Space Centre, India", "India"),
    "sdsc": ("SDSC-SHAR", "Satish Dhawan Space Centre, India", "India"),
    "tanegashima": ("Yoshinobu", "Tanegashima Space Center, Japan", "Japan"),
    "tnsc": ("Yoshinobu", "Tanegashima Space Center, Japan", "Japan"),
    "naro": ("NARO", "Naro Space Center, South Korea", "South Korea"),
    "semnan": ("Imam Khomeini", "Semnan, Iran", "Iran"),
    "palmachim": ("Palmachim", "Palmachim AB, Israel", "Israel"),
    "kodiak": ("Kodiak LP-1", "Pacific Spaceport Complex, Alaska, USA", "USA"),
    "sea launch": ("Odyssey", "Sea Launch, Pacific Ocean", "International"),
    "midway": ("Midway", "Pacific Ocean", "International"),
    "uaos": ("SLC-46", "Cape Canaveral SFS, Florida, USA", "USA"),
    "slc-46": ("SLC-46", "Cape Canaveral SFS, Florida, USA", "USA"),
    "lc-2": ("LC-2", "Wallops Island, Virginia, USA", "USA"),
}

# ---------------------------------------------------------------------------
# Country mapping (operator/rocket → country)
# ---------------------------------------------------------------------------
COUNTRY_BY_OPERATOR: dict[str, str] = {
    "spacex": "USA",
    "rocketlab": "New Zealand",
    "rocket lab": "New Zealand",
    "ula": "USA",
    "blue origin": "USA",
    "northrop grumman": "USA",
    "firefly": "USA",
    "relativity": "USA",
    "casc": "China",
    "casic": "China",
    "galactic energy": "China",
    "landspace": "China",
    "i-space": "China",
    "orienspace": "China",
    "space pioneer": "China",
    "roscosmos": "Russia",
    "isro": "India",
    "jaxa": "Japan",
    "kari": "South Korea",
    "arianegroup": "Europe",
    "avio": "Europe",
    "esa": "Europe",
    "irgc": "Iran",
    "isa": "Iran",
    "nada": "DPRK",
    "iai": "Israel",
}


def infer_country_from_rocket(rocket_name: str, rocket_family: str, operator: str) -> str:
    for key, country in COUNTRY_BY_OPERATOR.items():
        if key in operator.lower():
            return country
    return "Unknown"


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def normalize_rocket(text: str) -> dict:
    text_clean = re.sub(r"\[.*?\]", "", text).strip()
    for pattern, name, family, country, operator in _COMPILED_ROCKETS:
        if pattern.search(text_clean):
            return {
                "rocket_name": name,
                "family": family,
                "country": country,
                "operator": operator,
            }
    return {
        "rocket_name": text_clean[:80] if text_clean else "Unknown",
        "family": "Unknown",
        "country": "Unknown",
        "operator": "Unknown",
    }


def normalize_orbit(text: str) -> str:
    text_clean = re.sub(r"\[.*?\]", "", text).strip()
    for pattern, orbit in _COMPILED_ORBITS:
        if pattern.search(text_clean):
            return orbit
    return "LEO"  # default


def normalize_payload_type(mission_text: str, notes_text: str) -> str:
    combined = (mission_text + " " + notes_text).lower()
    for pattern, pt in _COMPILED_PAYLOAD_TYPES:
        if pattern.search(combined):
            return pt
    return "Unknown"


def normalize_launch_site(text: str) -> tuple[str, str, str]:
    """Return (site_name, site_location, site_country)."""
    t = text.lower()
    for key, (site, location, country) in LAUNCH_SITE_MAP.items():
        if key in t:
            return site, location, country
    # fallback: use raw text
    clean = re.sub(r"\[.*?\]", "", text).strip()
    return clean[:80], clean[:120], "Unknown"


def normalize_status(row_bg: Optional[str], notes: str) -> str:
    """Determine Success/Failure/Partial from row color and notes."""
    if row_bg:
        if "#cfc" in row_bg or "cfc" in row_bg.lower() or "ccffcc" in row_bg.lower():
            return "Success"
        if "#fcc" in row_bg or "fcc" in row_bg.lower() or "ffcccc" in row_bg.lower():
            return "Failure"
        if "#fc9" in row_bg or "fc9" in row_bg.lower() or "ffcc99" in row_bg.lower():
            return "Partial Failure"
        if "#ff9" in row_bg or "ff9" in row_bg.lower() or "ffff99" in row_bg.lower():
            return "Partial Success"
    notes_lower = notes.lower()
    if re.search(r"\bfailure\b|\bfailed\b|\blost\b|\bcrash\b|\bexplosion\b|\bdestroyed\b", notes_lower):
        return "Failure"
    if re.search(r"\bpartial\b|\babnormal\b|\bmalfunct", notes_lower):
        return "Partial Failure"
    return "Success"  # default assumption


def determine_payload_count(mission_name: str, payload_type: str, notes: str) -> int:
    """Estimate satellite/spacecraft count."""
    combined = mission_name + " " + notes

    # Starlink group batch sizes
    if re.search(r"starlink", combined, re.IGNORECASE):
        # Group X-Y notation — typically 22-23 sats per launch
        m = re.search(r"group\s*\d+[-–]\d+", combined, re.IGNORECASE)
        if m:
            return 23
        return 22

    # OneWeb
    if re.search(r"oneweb", combined, re.IGNORECASE):
        return 36

    # Amazon Kuiper
    if re.search(r"kuiper", combined, re.IGNORECASE):
        return 27  # typical batch

    # Rideshare: count comma-separated payloads
    if payload_type == "Rideshare":
        # Count mentions of satellite names (rough heuristic: count semicolons/commas after known separators)
        parts = re.split(r"[;,/]", mission_name)
        count = len([p for p in parts if p.strip()])
        return max(count, 1)

    # Crewed missions
    if re.search(r"crew[-\s]\d+|soyuz\s*ms|shenzhou", combined, re.IGNORECASE):
        return 1

    # Try to extract an explicit number
    m = re.search(r"(\d+)\s*(satellites?|sats?|spacecraft|cubesats?)", combined, re.IGNORECASE)
    if m:
        return int(m.group(1))

    return 1


def determine_operator_type(operator: str, mission_name: str, payload_type: str) -> str:
    commercial = ["spacex", "rocketlab", "rocket lab", "blue origin", "northrop", "ula",
                  "arianegroup", "arianespace", "avio", "oneweb", "starlink", "kuiper",
                  "galactic energy", "landspace", "i-space", "orienspace", "space pioneer",
                  "casic"]
    govt = ["isro", "jaxa", "roscosmos", "nasa", "casc", "kari", "isa", "irgc", "nada",
            "iai", "esa", "cnes"]

    combined = (operator + " " + mission_name).lower()
    for kw in commercial:
        if kw in combined:
            return "Commercial"
    for kw in govt:
        if kw in combined:
            return "Government"
    if payload_type in ("National Security",):
        return "Government"
    return "Commercial"  # default


def is_crewed_mission(mission_name: str, notes: str, payload_type: str) -> bool:
    combined = (mission_name + " " + notes).lower()
    return bool(
        re.search(
            r"\bcrew\b|\bastronauts?\b|\bcosmonaut\b|\btaikonaut\b|\bshenzhou\b|\bsoyuz\s*ms\b",
            combined,
        )
        or payload_type == "Crewed"
    )


def cell_text(cell: Optional[Tag]) -> str:
    if cell is None:
        return ""
    # Remove references, sup tags
    for sup in cell.find_all("sup"):
        sup.decompose()
    return cell.get_text(separator=" ", strip=True)


def get_row_bg(row: Tag) -> Optional[str]:
    """Extract background color from tr or first td style/bgcolor attribute."""
    style = row.get("style", "") or row.get("bgcolor", "")
    if not style:
        # Try first td
        td = row.find("td")
        if td:
            style = td.get("style", "") or td.get("bgcolor", "")
    if style:
        # Normalize hex colors
        m = re.search(r"background(?:-color)?\s*:\s*(#?[0-9a-fA-F]{3,8}|[a-z]+)", style)
        if m:
            return m.group(1).lower()
    return None


# ---------------------------------------------------------------------------
# Table flattening (handles rowspan/colspan)
# ---------------------------------------------------------------------------

def flatten_table(table: Tag) -> list[list[Optional[Tag]]]:
    """Flatten an HTML table to a 2D grid, respecting rowspan and colspan."""
    rows = table.find_all("tr")
    if not rows:
        return []
    max_cols = 0
    for row in rows:
        cells = row.find_all(["td", "th"])
        col_count = sum(int(c.get("colspan", 1)) for c in cells)
        if col_count > max_cols:
            max_cols = col_count
    if max_cols == 0:
        return []
    grid: list[list[Optional[Tag]]] = [[None] * max_cols for _ in range(len(rows))]
    for ri, row in enumerate(rows):
        ci = 0
        for cell in row.find_all(["td", "th"]):
            while ci < max_cols and grid[ri][ci] is not None:
                ci += 1
            if ci >= max_cols:
                break
            rs = int(cell.get("rowspan", 1))
            cs = int(cell.get("colspan", 1))
            for dr in range(rs):
                for dc in range(cs):
                    r, c = ri + dr, ci + dc
                    if r < len(rows) and c < max_cols:
                        grid[r][c] = cell
            ci += cs
    return grid


# ---------------------------------------------------------------------------
# Wikipedia fetch + parse
# ---------------------------------------------------------------------------

WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"

# Column indices for orbital launches table
# Order: Date, Time, Rocket, Launch Site, Payload, Orbit, Function, Operator, Notes
# Wikipedia tables vary by year; we detect columns by header text.
KNOWN_HEADERS = {
    "date": 0,
    "time": 1,
    "rocket": 2,
    "launch site": 3,
    "payload": 4,
    "orbit": 5,
    "function": 6,
    "operator": 7,
    "notes": 8,
    "outcome": 8,
}


def detect_column_map(header_row: list[Optional[Tag]]) -> dict[str, int]:
    """Given a header row, return a dict mapping field name -> column index."""
    col_map: dict[str, int] = {}
    for ci, cell in enumerate(header_row):
        if cell is None:
            continue
        text = cell_text(cell).lower().strip()
        if re.search(r"date|time\b", text) and "date" not in col_map:
            col_map["date"] = ci
        if re.search(r"\btime\b", text) and "time" not in col_map:
            col_map["time"] = ci
        if re.search(r"rocket|vehicle|launch\s*vehicle|booster", text) and "rocket" not in col_map:
            col_map["rocket"] = ci
        if re.search(r"launch\s*site|site|pad|complex|facility", text) and "launch_site" not in col_map:
            col_map["launch_site"] = ci
        if re.search(r"payload|spacecraft|mission|satellite", text) and "payload" not in col_map:
            col_map["payload"] = ci
        if re.search(r"orbit\b|orbital", text) and "orbit" not in col_map:
            col_map["orbit"] = ci
        if re.search(r"function|type\b|purpose", text) and "function" not in col_map:
            col_map["function"] = ci
        if re.search(r"operator|owner|customer", text) and "operator" not in col_map:
            col_map["operator"] = ci
        if re.search(r"notes?|remark|outcome|result|status", text) and "notes" not in col_map:
            col_map["notes"] = ci
    return col_map


def parse_date(raw: str, year: int) -> Optional[str]:
    """Try to parse a date string and return ISO format YYYY-MM-DD."""
    raw = re.sub(r"\[.*?\]", "", raw).strip()
    raw = re.sub(r"\s+", " ", raw)
    # Try common formats
    for fmt in (
        "%d %B %Y", "%B %d, %Y", "%Y-%m-%d",
        "%d %b %Y", "%b %d, %Y",
        "%B %d %Y", "%d %B", "%B %d",
    ):
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.year == 1900:
                dt = dt.replace(year=year)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Try partial match
    m = re.search(r"(\d{1,2})\s+([A-Za-z]+)", raw)
    if m:
        day, month_str = m.group(1), m.group(2)
        try:
            dt = datetime.strptime(f"{day} {month_str} {year}", "%d %B %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
        try:
            dt = datetime.strptime(f"{day} {month_str} {year}", "%d %b %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    # Try year-only
    m2 = re.search(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m2:
        return m2.group(0)
    return None


def fetch_wikipedia_year(year: int) -> Optional[str]:
    """Fetch Wikipedia 'YYYY in spaceflight' page HTML."""
    page_title = f"{year}_in_spaceflight"
    params = {
        "action": "parse",
        "page": page_title,
        "prop": "text",
        "format": "json",
        "redirects": 1,
    }
    url = f"{WIKIPEDIA_API_URL}?" + "&".join(f"{k}={v}" for k, v in params.items())
    resp = fetch_url(url)
    if not resp:
        write_fetch_log(f"wikipedia_{year}", "error", 0, "fetch failed after retries")
        return None
    try:
        data = resp.json()
        html = data["parse"]["text"]["*"]
        write_fetch_log(f"wikipedia_{year}", "success", 1)
        return html
    except (KeyError, json.JSONDecodeError) as exc:
        write_fetch_log(f"wikipedia_{year}", "error", 0, str(exc))
        warn(f"Failed to decode Wikipedia response for {year}: {exc}")
        return None


def find_orbital_launches_tables(soup: BeautifulSoup) -> list[Tag]:
    """Find all tables that look like orbital launch tables."""
    tables = []
    for h in soup.find_all(["h2", "h3"]):
        text = h.get_text(strip=True).lower()
        if "orbital" in text and "launch" in text:
            # find the next table(s) within the next sibling elements
            sib = h.find_next_sibling()
            while sib and sib.name not in ("h2",):
                if sib.name == "table":
                    tables.append(sib)
                    break
                if sib.name in ("div", "section"):
                    tbl = sib.find("table")
                    if tbl:
                        tables.append(tbl)
                        break
                sib = sib.find_next_sibling()
            if not tables:
                # broader search: all wikitables after this heading
                for tbl in soup.find_all("table", class_="wikitable"):
                    if tbl not in tables:
                        tables.append(tbl)
                break

    if not tables:
        # fallback: grab all wikitables
        vlog("No orbital section heading found; using all wikitables")
        tables = soup.find_all("table", class_=re.compile(r"wikitable"))

    return tables


def parse_orbital_launches(html: str, year: int) -> list[dict]:
    """Parse HTML and return list of launch records for the given year."""
    soup = BeautifulSoup(html, "lxml")
    tables = find_orbital_launches_tables(soup)
    vlog(f"Found {len(tables)} candidate tables for {year}")

    launches: list[dict] = []
    seq = 1

    for tbl_idx, table in enumerate(tables):
        grid = flatten_table(table)
        if not grid:
            vlog(f"  Table {tbl_idx}: empty grid, skipping")
            continue

        # Detect header row
        col_map: dict[str, int] = {}
        data_start_row = 0
        for ri, row in enumerate(grid):
            cells = [c for c in row if c is not None]
            if not cells:
                continue
            # Check if this looks like a header (th elements or recognizable header text)
            header_texts = [cell_text(c).lower() for c in cells if c is not None]
            if any(kw in " ".join(header_texts) for kw in ("rocket", "payload", "orbit", "launch vehicle")):
                col_map = detect_column_map(row)
                data_start_row = ri + 1
                vlog(f"  Table {tbl_idx}: header row {ri}, col_map={col_map}")
                break

        if not col_map:
            # Try first row as header anyway
            col_map = detect_column_map(grid[0])
            data_start_row = 1
            if not col_map:
                vlog(f"  Table {tbl_idx}: could not determine columns, skipping")
                continue

        # Ensure we have minimum required columns
        if "rocket" not in col_map and "payload" not in col_map:
            vlog(f"  Table {tbl_idx}: no rocket or payload column, skipping")
            continue

        # Process data rows
        prev_date = None
        for ri in range(data_start_row, len(grid)):
            row_cells = grid[ri]
            if all(c is None for c in row_cells):
                continue

            # Get raw text for each column
            def gcol(key: str, default_idx: Optional[int] = None) -> str:
                idx = col_map.get(key, default_idx)
                if idx is None or idx >= len(row_cells):
                    return ""
                return cell_text(row_cells[idx])

            date_raw = gcol("date")
            rocket_raw = gcol("rocket")
            site_raw = gcol("launch_site")
            payload_raw = gcol("payload")
            orbit_raw = gcol("orbit")
            function_raw = gcol("function")
            operator_raw = gcol("operator")
            notes_raw = gcol("notes")

            # Skip rows that look like sub-headers or empty
            if not rocket_raw and not payload_raw:
                continue
            if rocket_raw.lower() in ("rocket", "launch vehicle", "vehicle", ""):
                continue

            # Parse date
            date_str = None
            if date_raw:
                date_str = parse_date(date_raw, year)
                if date_str:
                    prev_date = date_str
                else:
                    date_str = prev_date
            else:
                date_str = prev_date

            if not date_str:
                date_str = f"{year}-01-01"  # fallback

            # Parse month/quarter from date
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                month = dt.month
                quarter = (month - 1) // 3 + 1
            except ValueError:
                month = 1
                quarter = 1

            # Rocket normalization
            rocket_info = normalize_rocket(rocket_raw)

            # Launch site
            site_name, site_location, site_country = normalize_launch_site(site_raw)

            # Orbit
            orbit_type = normalize_orbit(orbit_raw or function_raw)

            # Payload type
            payload_type = normalize_payload_type(payload_raw + " " + function_raw, notes_raw)

            # Status
            # get row background from the actual tr element
            row_tr = None
            if row_cells:
                for c in row_cells:
                    if c is not None:
                        row_tr = c.parent
                        break
            row_bg = get_row_bg(row_tr) if row_tr else None
            status = normalize_status(row_bg, notes_raw)

            # Payload count
            payload_count = determine_payload_count(payload_raw, payload_type, notes_raw)

            # Operator type
            op_type = determine_operator_type(
                operator_raw or rocket_info["operator"], payload_raw, payload_type
            )

            # Crewed
            crewed = is_crewed_mission(payload_raw, notes_raw, payload_type)

            # Country: prefer rocket info, override with operator info
            country = rocket_info["country"]
            if country == "Unknown" and site_country != "Unknown":
                country = site_country

            # Map country to dashboard country keys
            country = map_country_to_dashboard(country, rocket_info["operator"])

            record: dict[str, Any] = {
                "id": f"{year}-{seq:03d}",
                "date": date_str,
                "year": year,
                "month": month,
                "quarter": quarter,
                "country": country,
                "provider": rocket_info["operator"],
                "rocket": rocket_info["rocket_name"],
                "rocket_family": rocket_info["family"],
                "launch_site": site_name,
                "launch_site_location": site_location,
                "launch_site_country": site_country,
                "mission_name": re.sub(r"\[.*?\]", "", payload_raw).strip()[:200],
                "orbit": orbit_type,
                "orbit_type": "orbital",
                "payload_count": payload_count,
                "payload_mass_kg": None,
                "payload_type": payload_type,
                "operator_type": op_type,
                "status": status,
                "is_crewed": crewed,
                "notes": re.sub(r"\[.*?\]", "", notes_raw).strip()[:500],
                "source_url": f"https://en.wikipedia.org/wiki/{year}_in_spaceflight",
            }
            launches.append(record)
            seq += 1

        vlog(f"  Table {tbl_idx}: parsed {seq - 1} launches so far")

    return launches


def map_country_to_dashboard(country: str, operator: str) -> str:
    """Map raw country to the dashboard's country key set."""
    mapping = {
        "USA": "USA",
        "United States": "USA",
        "China": "China",
        "Russia": "Russia",
        "India": "India",
        "Europe": "Europe",
        "France": "Europe",
        "European": "Europe",
        "New Zealand": "New Zealand",
        "South Korea": "South Korea",
        "Korea": "South Korea",
        "Japan": "Japan",
        "Iran": "Others",
        "DPRK": "Others",
        "North Korea": "Others",
        "Israel": "Others",
        "International": "Others",
        "Unknown": "Others",
    }
    result = mapping.get(country, None)
    if result:
        return result
    # Try operator-based lookup
    op_lower = operator.lower()
    if any(kw in op_lower for kw in ("spacex", "ula", "blue origin", "northrop", "nasa", "firefly", "relativity", "abl")):
        return "USA"
    if any(kw in op_lower for kw in ("casc", "casic", "galactic energy", "landspace", "i-space", "orienspace", "space pioneer")):
        return "China"
    if any(kw in op_lower for kw in ("roscosmos",)):
        return "Russia"
    if any(kw in op_lower for kw in ("isro",)):
        return "India"
    if any(kw in op_lower for kw in ("arianegroup", "arianespace", "avio", "esa")):
        return "Europe"
    if any(kw in op_lower for kw in ("rocketlab", "rocket lab")):
        return "New Zealand"
    if any(kw in op_lower for kw in ("kari",)):
        return "South Korea"
    if any(kw in op_lower for kw in ("jaxa",)):
        return "Japan"
    return "Others"


# ---------------------------------------------------------------------------
# Supplementary RSS sources
# ---------------------------------------------------------------------------

def fetch_rss(url: str, source_name: str) -> list[dict]:
    """Fetch and parse an RSS feed, return list of item dicts."""
    resp = fetch_url(url)
    if not resp:
        write_fetch_log(source_name, "error", 0, "fetch failed")
        return []
    try:
        root = ET.fromstring(resp.content)
        ns = {}
        items = root.findall(".//item") or root.findall(".//{http://www.w3.org/2005/Atom}entry")
        results = []
        for item in items:
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pubdate = (item.findtext("pubDate") or item.findtext("published") or "").strip()
            desc = (item.findtext("description") or item.findtext("summary") or "").strip()
            results.append({"title": title, "link": link, "pubdate": pubdate, "description": desc[:300]})
        write_fetch_log(source_name, "success", len(results))
        return results
    except ET.ParseError as exc:
        write_fetch_log(source_name, "error", 0, str(exc))
        warn(f"RSS parse error for {source_name}: {exc}")
        return []


def fetch_supplementary_sources() -> dict:
    """Fetch supplementary data from NASA, ESA, and SpaceX."""
    log("Fetching supplementary sources...")
    sources: dict[str, Any] = {}

    # NASA RSS
    log("  NASA News RSS...")
    nasa_items = fetch_rss("https://www.nasa.gov/news-releases/feed/", "nasa_news_rss")
    sources["nasa_news"] = {
        "url": "https://www.nasa.gov/news-releases/feed/",
        "type": "rss",
        "count": len(nasa_items),
        "items": nasa_items[:20],  # keep first 20 for metadata
    }
    time.sleep(1)

    # ESA RSS
    log("  ESA newsroom RSS...")
    esa_items = fetch_rss(
        "https://www.esa.int/rssfeed/Our_Activities/Space_Transportation",
        "esa_transport_rss",
    )
    sources["esa_space_transport"] = {
        "url": "https://www.esa.int/rssfeed/Our_Activities/Space_Transportation",
        "type": "rss",
        "count": len(esa_items),
        "items": esa_items[:20],
    }
    time.sleep(1)

    # SpaceX launches page (informational, not structured)
    log("  SpaceX launches page...")
    resp = fetch_url("https://www.spacex.com/launches/")
    if resp:
        sources["spacex_manifests"] = {
            "url": "https://www.spacex.com/launches/",
            "type": "webpage",
            "status": "fetched",
            "content_length": len(resp.text),
        }
        write_fetch_log("spacex_launches_page", "success", 1)
    else:
        sources["spacex_manifests"] = {
            "url": "https://www.spacex.com/launches/",
            "type": "webpage",
            "status": "failed",
        }
        write_fetch_log("spacex_launches_page", "error", 0)

    return sources


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

DASHBOARD_COUNTRIES = [
    "USA", "China", "Russia", "India", "Europe", "Japan", "New Zealand", "South Korea", "Others"
]


def aggregate_launches(launches: list[dict], year: int) -> dict:
    """Build per-year aggregated data from per-launch records."""
    countries_data: dict[str, Any] = {}

    for country in DASHBOARD_COUNTRIES:
        country_launches = [l for l in launches if l["country"] == country]
        if not country_launches:
            countries_data[country] = {
                "monthly": [0] * 12,
                "total": 0,
                "rocket_families": {},
                "orbit_types": {},
                "payload_types": {},
                "operator_types": {},
                "success_rate": 100.0,
                "monthly_satellites": [0] * 12,
            }
            continue

        monthly = [0] * 12
        monthly_sats = [0] * 12
        rocket_families: dict[str, int] = {}
        orbit_types: dict[str, int] = {}
        payload_types: dict[str, int] = {}
        operator_types: dict[str, int] = {}
        success_count = 0

        for launch in country_launches:
            m = launch["month"] - 1  # 0-indexed
            if 0 <= m < 12:
                monthly[m] += 1
                monthly_sats[m] += launch.get("payload_count", 1)

            rf = launch.get("rocket_family", "Unknown")
            rocket_families[rf] = rocket_families.get(rf, 0) + 1

            ot = launch.get("orbit", "LEO")
            orbit_types[ot] = orbit_types.get(ot, 0) + 1

            pt = launch.get("payload_type", "Unknown")
            payload_types[pt] = payload_types.get(pt, 0) + 1

            opty = launch.get("operator_type", "Commercial")
            operator_types[opty] = operator_types.get(opty, 0) + 1

            if launch.get("status", "").lower().startswith("success"):
                success_count += 1

        total = len(country_launches)
        success_rate = round((success_count / total) * 100, 1) if total > 0 else 100.0

        # Sort dicts by value descending
        rocket_families = dict(sorted(rocket_families.items(), key=lambda x: -x[1]))
        orbit_types = dict(sorted(orbit_types.items(), key=lambda x: -x[1]))
        payload_types = dict(sorted(payload_types.items(), key=lambda x: -x[1]))
        operator_types = dict(sorted(operator_types.items(), key=lambda x: -x[1]))

        countries_data[country] = {
            "monthly": monthly,
            "total": total,
            "rocket_families": rocket_families,
            "orbit_types": orbit_types,
            "payload_types": payload_types,
            "operator_types": operator_types,
            "success_rate": success_rate,
            "monthly_satellites": monthly_sats,
        }

    total_launches = len(launches)
    return {
        "status": "actual",
        "actual_months": 12,
        "total": total_launches,
        "countries": countries_data,
    }


def aggregate_legacy_format(launches: list[dict], year: int) -> dict:
    """Build the simple monthly-array format used by existing launches.json."""
    result: dict[str, list[int]] = {c: [0] * 12 for c in DASHBOARD_COUNTRIES}
    for launch in launches:
        country = launch.get("country", "Others")
        if country not in result:
            country = "Others"
        m = launch.get("month", 1) - 1
        if 0 <= m < 12:
            result[country][m] += 1
    return result


# ---------------------------------------------------------------------------
# Q4 2026 Prediction
# ---------------------------------------------------------------------------

def predict_q4_2026(all_year_data: dict[int, list[dict]]) -> dict:
    """
    Use Q4 data from 2023-2025 to predict Q4 2026.
    Returns prediction dict with per-country predictions and confidence ranges.
    """
    # Gather Q4 (months 10,11,12 = indices 9,10,11) totals per country per year
    prediction: dict[str, Any] = {}
    available_years = sorted([y for y in all_year_data if y in (2023, 2024, 2025) and all_year_data[y]])

    if len(available_years) < 2:
        warn("Not enough years for Q4 prediction (need at least 2 of 2023/2024/2025)")
        return {}

    country_predictions: dict[str, Any] = {}
    for country in DASHBOARD_COUNTRIES:
        q4_totals: list[tuple[int, int]] = []
        for yr in available_years:
            launches_yr = all_year_data.get(yr, [])
            q4_count = sum(
                1 for l in launches_yr
                if l.get("country") == country and l.get("month", 0) in (10, 11, 12)
            )
            q4_totals.append((yr, q4_count))

        if not q4_totals:
            country_predictions[country] = {"predicted": 0, "low": 0, "high": 0, "basis": "no data"}
            continue

        # Linear trend: y = a + b*x where x is years since first year
        n = len(q4_totals)
        xs = [t[0] for t in q4_totals]
        ys = [t[1] for t in q4_totals]

        if n == 1:
            predicted = ys[0]
        else:
            # Least-squares linear fit
            x_mean = sum(xs) / n
            y_mean = sum(ys) / n
            numerator = sum((xs[i] - x_mean) * (ys[i] - y_mean) for i in range(n))
            denominator = sum((xs[i] - x_mean) ** 2 for i in range(n))
            if denominator == 0:
                predicted = y_mean
            else:
                slope = numerator / denominator
                intercept = y_mean - slope * x_mean
                predicted = intercept + slope * 2026

        predicted = max(0, round(predicted))
        confidence = 0.15
        low = max(0, round(predicted * (1 - confidence)))
        high = round(predicted * (1 + confidence))

        country_predictions[country] = {
            "predicted": predicted,
            "low": low,
            "high": high,
            "historical": dict(q4_totals),
            "trend": "linear regression on Q4 totals",
            "confidence": "±15%",
        }

    total_predicted = sum(v["predicted"] for v in country_predictions.values())
    total_low = sum(v["low"] for v in country_predictions.values())
    total_high = sum(v["high"] for v in country_predictions.values())

    return {
        "method": "linear_trend_q4",
        "basis_years": available_years,
        "description": "Q4 (Oct-Dec) 2026 orbital launch prediction based on linear trend from prior years",
        "total_predicted": total_predicted,
        "total_low": total_low,
        "total_high": total_high,
        "confidence": "±15%",
        "countries": country_predictions,
    }


# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------

def load_existing_launches_json() -> dict:
    if os.path.exists(LAUNCHES_JSON):
        with open(LAUNCHES_JSON, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError as exc:
                warn(f"Could not parse existing launches.json: {exc}")
    return {}


def write_json_file(path: str, data: Any, dry_run: bool = False) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    content = json.dumps(data, indent=2, ensure_ascii=False)
    if dry_run:
        log(f"[dry-run] Would write {len(content):,} bytes to {path}")
    else:
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        log(f"Wrote {len(content):,} bytes to {path}")


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def build_launches_json(
    all_launches: dict[int, list[dict]],
    existing: dict,
    supplementary: dict,
    q4_prediction: dict,
    actual_months_2026: int = 4,
) -> dict:
    """
    Build the updated launches.json structure, preserving existing format
    while adding richer fields.
    """
    # Start with existing structure to preserve metadata/countries list
    result = dict(existing) if existing else {}

    # Preserve or rebuild metadata
    if "metadata" not in result:
        result["metadata"] = {}

    result["metadata"].update(
        {
            "title": "Global Space Launch Statistics",
            "description": "Orbital launch attempts by country, 2023-2026",
            "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "current_year": 2026,
            "actual_months_2026": actual_months_2026,
            "sources": [
                "Wikipedia (citing primary sources: NASA, ESA, Roscosmos, CASC, JAXA, etc.)",
                "NASA News RSS",
                "ESA Newsroom RSS",
                "SpaceX Manifests",
            ],
            "months": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
            "supplementary_sources": supplementary,
        }
    )

    # Preserve existing countries list
    if "countries" not in result:
        result["countries"] = [
            {"id": "USA", "iso3": "USA", "flag": "🇺🇸", "color": "#3b82f6", "label": "United States"},
            {"id": "China", "iso3": "CHN", "flag": "🇨🇳", "color": "#ef4444", "label": "China"},
            {"id": "Russia", "iso3": "RUS", "flag": "🇷🇺", "color": "#f97316", "label": "Russia"},
            {"id": "India", "iso3": "IND", "flag": "🇮🇳", "color": "#22c55e", "label": "India"},
            {"id": "Europe", "iso3": "FRA", "flag": "🇪🇺", "color": "#a855f7", "label": "Europe (ESA/Ariane)"},
            {"id": "Japan", "iso3": "JPN", "flag": "🇯🇵", "color": "#ec4899", "label": "Japan"},
            {"id": "New Zealand", "iso3": "NZL", "flag": "🇳🇿", "color": "#f59e0b", "label": "New Zealand (RocketLab)"},
            {"id": "South Korea", "iso3": "KOR", "flag": "🇰🇷", "color": "#06b6d4", "label": "South Korea"},
            {"id": "Others", "iso3": None, "flag": "🌐", "color": "#6b7280", "label": "Others (Iran, DPRK, etc.)"},
        ]

    # Build launches section
    if "launches" not in result:
        result["launches"] = {}

    for year, launches_list in all_launches.items():
        if not launches_list:
            log(f"  Year {year}: 0 launches parsed; preserving existing data if present")
            continue

        legacy_countries = aggregate_legacy_format(launches_list, year)
        rich_agg = aggregate_launches(launches_list, year)

        is_partial = (year == 2026)
        actual_m = actual_months_2026 if is_partial else 12

        year_entry: dict[str, Any] = {
            "status": "partial" if is_partial else "actual",
            "actual_months": actual_m,
            "total": rich_agg["total"],
            "countries": legacy_countries,  # backward-compat: plain monthly arrays
            "rich": rich_agg["countries"],  # richer per-country data
        }

        if is_partial:
            # If we have existing predicted data for months beyond actual, preserve it
            existing_year = (result.get("launches") or {}).get(str(year), {})
            existing_countries_monthly = existing_year.get("countries", {})
            for country in DASHBOARD_COUNTRIES:
                new_monthly = legacy_countries.get(country, [0] * 12)
                old_monthly = existing_countries_monthly.get(country, [0] * 12)
                # Merge: use parsed data for actual months, existing for predicted
                merged = list(new_monthly)
                for mi in range(actual_m, 12):
                    if mi < len(old_monthly):
                        merged[mi] = old_monthly[mi]
                    else:
                        merged[mi] = 0
                year_entry["countries"][country] = merged
            year_entry["note"] = f"Months 1-{actual_m} are actual; months {actual_m+1}-12 are predicted"

        result["launches"][str(year)] = year_entry

    # Add Q4 2026 prediction
    if q4_prediction:
        result["q4_2026_prediction"] = q4_prediction

    return result


def build_detailed_json(
    all_launches: dict[int, list[dict]],
    supplementary: dict,
) -> dict:
    """Build launches_detailed.json with per-launch records and metadata."""
    all_records: list[dict] = []
    for year in sorted(all_launches.keys()):
        all_records.extend(all_launches[year])

    total = len(all_records)
    return {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_launches": total,
            "schema_version": "2.0",
            "years_covered": sorted(all_launches.keys()),
            "source": "Wikipedia 'X in spaceflight' pages (citing primary sources)",
            "supplementary_sources": supplementary,
        },
        "launches": all_records,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    global VERBOSE

    parser = argparse.ArgumentParser(
        description="Fetch orbital launch data from Wikipedia and write JSON for Flask dashboard"
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=[2023, 2024, 2025, 2026],
        help="Years to fetch (default: 2023 2024 2025 2026)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be written without writing files",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    args = parser.parse_args()

    VERBOSE = args.verbose
    dry_run = args.dry_run

    if dry_run:
        log("DRY RUN mode: no files will be written")

    os.makedirs(DATA_DIR, exist_ok=True)

    # Step 1: Fetch Wikipedia data for each year
    all_launches: dict[int, list[dict]] = {}
    for year in args.years:
        log(f"Fetching Wikipedia: {year} in spaceflight...")
        html = fetch_wikipedia_year(year)
        if html:
            log(f"  Parsing tables for {year}...")
            launches = parse_orbital_launches(html, year)
            log(f"  Parsed {len(launches)} launches for {year}")
            if len(launches) == 0:
                warn(f"  0 launches parsed for {year}; check table structure")
                write_fetch_log(f"wikipedia_parse_{year}", "warn", 0, "0 rows parsed")
            all_launches[year] = launches
            write_fetch_log(f"wikipedia_parse_{year}", "success", len(launches))
        else:
            warn(f"  Failed to fetch Wikipedia page for {year}; skipping")
            all_launches[year] = []
        # Rate limit: 1s between Wikipedia API calls
        time.sleep(1)

    total_parsed = sum(len(v) for v in all_launches.values())
    log(f"Total launches parsed across all years: {total_parsed}")

    # Step 2: Fetch supplementary sources
    supplementary = fetch_supplementary_sources()

    # Step 3: Build Q4 2026 prediction
    log("Computing Q4 2026 prediction...")
    q4_prediction = predict_q4_2026(all_launches)
    if q4_prediction:
        log(
            f"  Q4 2026 predicted total: {q4_prediction.get('total_predicted', 'N/A')} "
            f"(range: {q4_prediction.get('total_low', 'N/A')}–{q4_prediction.get('total_high', 'N/A')})"
        )

    # Step 4: Load existing data
    existing = load_existing_launches_json()

    # Step 5: Build and write launches.json
    log("Building launches.json...")
    launches_json = build_launches_json(
        all_launches, existing, supplementary, q4_prediction
    )
    write_json_file(LAUNCHES_JSON, launches_json, dry_run=dry_run)

    # Step 6: Build and write launches_detailed.json
    log("Building launches_detailed.json...")
    detailed_json = build_detailed_json(all_launches, supplementary)
    write_json_file(LAUNCHES_DETAILED_JSON, detailed_json, dry_run=dry_run)

    # Summary
    log("Done.")
    for year in sorted(all_launches.keys()):
        count = len(all_launches[year])
        if count:
            countries_seen = set(l["country"] for l in all_launches[year])
            log(f"  {year}: {count} launches, countries: {', '.join(sorted(countries_seen))}")
        else:
            log(f"  {year}: 0 launches (fell back to existing data)")


if __name__ == "__main__":
    main()
