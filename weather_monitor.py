#!/usr/bin/env python3
"""
Severe Weather Monitor (2x/day, rolling 7-day)

Fixes:
- Open-Meteo 'freezing_rain_sum' caused 400 for many locations -> removed.
- Use one Open-Meteo call per site with widely supported fields:
  snowfall_sum, precipitation_sum, temperature_2m_min
- Ice accumulation is an ESTIMATE (proxy) based on subfreezing temps + precip.
- Open-Meteo timeouts no longer fail the entire site; site returns with LOW confidence instead.

Outputs:
- weather_warning_report.md
- weather_warning_report.csv

Notifications (optional):
- Microsoft Teams webhook (TEAMS_WEBHOOK_URL)
- Email via SMTP (SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, EMAIL_TO, EMAIL_FROM)

Safe to run with NO secrets:
- Blank TEAMS_WEBHOOK_URL -> no Teams post
- Blank SMTP_* / EMAIL_* -> no email
"""

import csv
import json
import os
import time
import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests
import xml.etree.ElementTree as ET
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


# =========================
# SAFE ENV HELPERS
# =========================

def env_str(name: str, default: str = "") -> str:
    v = os.getenv(name, default)
    return (v if v is not None else "").strip()

def env_float(name: str, default: float) -> float:
    raw = env_str(name, "")
    if not raw:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)

def env_int(name: str, default: int) -> int:
    raw = env_str(name, "")
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


# =========================
# CONFIG
# =========================

# Thresholds (7-day totals)
SNOW_HEADSUP_IN = env_float("SNOW_HEADSUP_IN", 2.0)
SNOW_WARNING_IN = env_float("SNOW_WARNING_IN", 4.0)
SNOW_CRITICAL_IN = env_float("SNOW_CRITICAL_IN", 8.0)

ICE_HEADSUP_IN = env_float("ICE_HEADSUP_IN", 0.05)
ICE_WARNING_IN = env_float("ICE_WARNING_IN", 0.10)
ICE_CRITICAL_IN = env_float("ICE_CRITICAL_IN", 0.25)

# Ice proxy tuning:
# ice_in ≈ (precip_mm / 25.4) * ICE_PROXY_FACTOR  when temp_min <= 0C and snow is low
ICE_PROXY_FACTOR = env_float("ICE_PROXY_FACTOR", 0.35)
ICE_PROXY_MAX_IN_PER_DAY = env_float("ICE_PROXY_MAX_IN_PER_DAY", 0.35)  # cap daily ice estimate

LOCAL_TZ_LABEL = env_str("LOCAL_TZ_LABEL", "America/Chicago")

# NWS requires a real User-Agent
NWS_USER_AGENT = env_str(
    "NWS_USER_AGENT",
    "PrIME-SevereWeatherMonitor/1.0 (contact: you@company.com)",
)

REQUEST_TIMEOUT = env_int("REQUEST_TIMEOUT", 25)
RETRY_SLEEP_SEC = env_float("RETRY_SLEEP_SEC", 1.2)
MAX_RETRIES = env_int("MAX_RETRIES", 4)

# Optional notifications
TEAMS_WEBHOOK_URL = env_str("TEAMS_WEBHOOK_URL", "")

SMTP_HOST = env_str("SMTP_HOST", "")
SMTP_PORT = env_int("SMTP_PORT", 587)
SMTP_USER = env_str("SMTP_USER", "")
SMTP_PASS = env_str("SMTP_PASS", "")
EMAIL_TO = env_str("EMAIL_TO", "")          # comma-separated
EMAIL_FROM = env_str("EMAIL_FROM", "")
EMAIL_SUBJECT_PREFIX = env_str("EMAIL_SUBJECT_PREFIX", "[Severe Wx] ")

# Output files
OUT_MD = env_str("OUT_MD", "weather_warning_report.md")
OUT_CSV = env_str("OUT_CSV", "weather_warning_report.csv")


# =========================
# SITES (paste/edit freely)
# =========================
SITES_CSV = r"""site_name,country,prime_status,lat,lon,site_code,application,bu,address,eccc_feed_url
Brantford - BRMC,Canada,Active,43.164623,-80.341370,7064,PrIME,NA SMO,"59 Fen Ridge Ct. Brantford ON N3V1G2",
Greater Chicago Fulfillment Center - GCFC,United States,Active,41.430267,-88.426704,D488,PrIME,NA SMO,"222 Airport Road, Morris, Illinois 60450",
Dayton Fulfillment Center - DYFC,United States,Active,39.876823,-84.311685,B360,PrIME,NA SMO,"1800 Union Airpark Boulevard, Union Ohio, 45377",
Northeast Fulfillment Center - NEFC,United States,Active,40.027993,-77.525772,B278,PrIME,NA SMO,"9300 Olde Scotland Rd Shippensburg, PA 18629",
Tabler Station (FP),United States,Active,39.214575,-77.963890,B680,PrIME,GLOBAL HAIR CARE,"396 Development Drive Inwood, WV 25428",
Albany,United States,Active,31.558926,-84.114466,U018,PrIME,GLOBAL FAMILY CARE,"512 Liberty Expressway-S.E. Albany, GA 31705",
Dallas Fulfillment Center - DLMC,United States,Active,32.576484,-96.676480,B341,PrIME,NA SMO,"101 Mars Rd, Wilmer, TX 75172",
West Coast Fulfillment Center - WCMC,United States,Active,33.884275,-117.239226,B275,PrIME,NA SMO,"16110 Cosmos St Moreno Valley, CA 92551",
Green Bay,United States,Coming Soon!,44.521507,-88.001472,U020,RTCIS,GLOBAL FAMILY CARE,"501 Eastman Ave, Green Bay, WI 54302",
St. Louis,United States,Active,38.676231,-90.198237,1731,PrIME,,"169 East Grand Ave. St. Louis, MO 63147",
Box Elder,United States,Active,41.616368,-112.178286,9665,PrIME,GLOBAL FAMILY CARE,"5000 N 6800 W Iowa String Rd.  Bear River City, UT 84301",
Lima,United States,In Progress,40.742284,-84.082289,1702,RTCIS,GLOBAL FABRIC CARE,"3875 Reservoir Rd. Lima, Oh 45801",
Cape Girardeau,United States,In Progress,37.434218,-89.634617,U017,RTCIS,GLOBAL BABY CARE,"14484 State Highway 177 Jackson, MO 63755",
Southeast Fulfillment Center - SEFC,United States,Active,33.582844,-84.511476,D941,PrIME,NA SMO,"950 Logistics Parkway Jackson, GA 30233",
Mehoopany,United States,In Progress,41.540685,-76.103227,U011,RTCIS,GLOBAL FAMILY CARE,"Route 87 Mehoopany Mehoopany, PA 18629",
Oxnard,United States,Coming Soon!,34.218747,-119.142037,U019,RTCIS,GLOBAL FAMILY CARE,"800 Rice Ave, Oxnard, CA 93030",
Dover,United States,Coming Soon!,39.151860,-75.545158,1843,RTCIS,GLOBAL BABY CARE,"1340 W North St, Dover, DE 19904",
Phoenix,United States,In Progress,33.428605,-112.134451,1725,PrIME,GLOBAL PERSONAL HEALTH CARE,"2050 S 35th Ave.  Phoenix, AZ 85009",
GBO-BS,United States,Active,36.177070,-79.729299,1707,PrIME,GLOBAL ORAL CARE,"6200 Bryan Park Rd., Browns Summit, NC 27214",
GBO-SR,United States,Coming Soon!,36.071720,-79.910100,1719,RTCIS,GLOBAL PERSONAL HEALTH CARE,"100 S Swing Rd., Greensboro, NC 27409",
Iowa City - Oral B,United States,Coming Soon!,41.640603,-91.501216,8335,RTCIS,GLOBAL ORAL CARE,"2200 Lower Muscatine Rd, Iowa City, IA 52240",
Iowa City - Hair Care,United States,In Progress,41.640603,-91.501216,5395,RTCIS,GLOBAL HAIR CARE,"2200 Lower Muscatine Rd, Iowa City, IA 52240",
Auburn,United States,Coming Soon!,44.037222,-70.282110,2430,RTCIS,GLOBAL FEM CARE,"2879 Hotel Rd Auburn, ME 04210",
Belleville,Canada,In Progress,44.198163,-77.366710,4786,RTCIS,GLOBAL FEM CARE,"355 University Ave Belleville, ON K8N 5T8",
Alexandria,United States,Coming Soon!,31.364839,-92.411079,1727,RTCIS,GLOBAL FABRIC CARE,"3701 Monroe Highway Pineville, LA 71360",
Tabler Station (RPM),United States,Coming Soon!,39.214575,-77.963890,B680,RTCIS,GLOBAL HAIR CARE,"396 Development Drive Inwood, WV 25428",
Andover,United States,Active,42.612113,-71.173319,8323,PrIME,GLOBAL SHAVE CARE,"30 Burtt Rd, Andover, MA 01810",
Mielle,United States,Active,41.459510,-87.319000,E417,PrIME,GLOBAL HAIR CARE,"8707 Louisiana St. Merrillville, IN 46410",
La Muda,United States,In Progress,18.415300,-66.059400,8503,RTCIS,NA SMO,"San Juan, PR 00901",
Aero Fulflmnt-Cinci-PGDIS,United States,3PL Connect,39.340431,-84.290115,1530,3PL Connect,GLOBAL ORAL CARE,"3900 Aero Drive Mason, OH 45040",
KIK - Elkhart IN,United States,3PL Connect,41.689285,-85.948952,1445,3PL Connect,NA SMO,"1919 Superior St. Elkhart, IN 46516",
Nickey Memphis,United States,3PL Connect,35.060078,-90.054047,E472,3PL Connect,,"319 Titan Dr. Memphis, TN 38109",
Peter Cremer - Cincinnati,United States,3PL Connect,39.084077,-84.572226,4675,3PL Connect,NA SMO,"3117 Southside Ave Cincinnati, OH 45204",
Sacramento Plant,United States,3PL Connect,38.525189,-121.402174,1730,3PL Connect,GLOBAL FABRIC CARE,"8201 Fruitridge Road Sacramento, CA 95826",
WCMC Club OSW,United States,3PL Connect,34.037869,-117.632970,D484,3PL Connect,NA SMO,"1990 S Cucamonga Ave Ontario, CA 91761",
Zobele - Garland - PGMFG,United States,3PL Connect,32.887953,-96.684286,C454,3PL Connect,NA SMO,"3502 Regency Crest Dr Garland, TX 75041",
Edwardsville Mixing Center - EDMC,United States,Coming Soon!,38.770279,-90.050120,2585,RTCIS,NA SMO,"3049 Westway Drive Edwardsville, IL 62024",
"""


# =========================
# DATA TYPES
# =========================

@dataclass
class Site:
    site_name: str
    country: str
    prime_status: str
    lat: float
    lon: float
    site_code: str
    application: str
    bu: str
    address: str
    eccc_feed_url: str = ""


@dataclass
class AlertItem:
    title: str
    starts: Optional[str] = None
    ends: Optional[str] = None
    severity: Optional[str] = None
    source: str = ""


@dataclass
class SiteResult:
    site_code: str
    site_name: str
    country: str
    prime_status: str
    lat: float
    lon: float

    alerts: List[AlertItem]
    snow_7d_in: float
    ice_7d_in: float
    daily_snow_in: List[float]  # len 7
    daily_ice_in: List[float]   # len 7

    risk_level: str  # NONE / HEADSUP / WARNING / CRITICAL
    risk_reason: str
    confidence: str  # HIGH / MEDIUM / LOW
    address: str = ""


# =========================
# HTTP HELPERS
# =========================

def http_get_json(url: str, headers: Optional[dict] = None) -> dict:
    last_err = None
    for i in range(MAX_RETRIES):
        try:
            r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(RETRY_SLEEP_SEC * (1 + i))
    raise RuntimeError(f"GET JSON failed: {url} :: {last_err}")


def http_get_text(url: str, headers: Optional[dict] = None) -> str:
    last_err = None
    for i in range(MAX_RETRIES):
        try:
            r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(RETRY_SLEEP_SEC * (1 + i))
    raise RuntimeError(f"GET TEXT failed: {url} :: {last_err}")


# =========================
# SITE LOADING
# =========================

def load_sites_from_embedded_csv(csv_text: str) -> List[Site]:
    reader = csv.DictReader(csv_text.strip().splitlines())
    sites: List[Site] = []
    for row in reader:
        sites.append(
            Site(
                site_name=(row.get("site_name") or "").strip(),
                country=(row.get("country") or "").strip(),
                prime_status=(row.get("prime_status") or "").strip(),
                lat=float(row.get("lat") or 0.0),
                lon=float(row.get("lon") or 0.0),
                site_code=(row.get("site_code") or "").strip(),
                application=(row.get("application") or "").strip(),
                bu=(row.get("bu") or "").strip(),
                address=(row.get("address") or "").strip(),
                eccc_feed_url=(row.get("eccc_feed_url") or "").strip(),
            )
        )
    return sites


# =========================
# RISK LOGIC
# =========================

def classify_risk(snow_in: float, ice_in: float, has_alerts: bool) -> Tuple[str, str]:
    if has_alerts:
        return "WARNING", "Active official alert(s) present"

    if snow_in >= SNOW_CRITICAL_IN or ice_in >= ICE_CRITICAL_IN:
        return "CRITICAL", "Forecast accumulation exceeds critical threshold"
    if snow_in >= SNOW_WARNING_IN or ice_in >= ICE_WARNING_IN:
        return "WARNING", "Forecast accumulation exceeds warning threshold"
    if snow_in >= SNOW_HEADSUP_IN or ice_in >= ICE_HEADSUP_IN:
        return "HEADSUP", "Forecast accumulation exceeds heads-up threshold"

    return "NONE", "No alerts and accumulation below thresholds"


# =========================
# US: NWS ALERTS
# =========================

def fetch_nws_alerts(lat: float, lon: float) -> List[AlertItem]:
    url = f"https://api.weather.gov/alerts/active?point={lat:.6f},{lon:.6f}"
    data = http_get_json(
        url,
        headers={"User-Agent": NWS_USER_AGENT, "Accept": "application/geo+json"},
    )
    alerts: List[AlertItem] = []
    for feat in (data.get("features") or [])[:50]:
        props = feat.get("properties") or {}
        title = props.get("headline") or props.get("event") or "Alert"
        starts = props.get("effective") or props.get("onset")
        ends = props.get("ends") or props.get("expires")
        severity = props.get("severity")
        alerts.append(AlertItem(title=title, starts=starts, ends=ends, severity=severity, source="NWS"))
    return alerts


# =========================
# OPEN-METEO (single call per site)
# =========================

def fetch_open_meteo_daily(lat: float, lon: float) -> Dict[str, List[float]]:
    """
    Returns dict of lists length up to 7:
      - snowfall_sum (cm)
      - precipitation_sum (mm)
      - temperature_2m_min (C)
    """
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat:.6f}&longitude={lon:.6f}"
        "&daily=snowfall_sum,precipitation_sum,temperature_2m_min"
        "&forecast_days=7"
        "&timezone=UTC"
    )
    data = http_get_json(url, headers={"Accept": "application/json"})
    daily = data.get("daily") or {}
    return {
        "snowfall_sum": daily.get("snowfall_sum") or [0.0] * 7,
        "precipitation_sum": daily.get("precipitation_sum") or [0.0] * 7,
        "temperature_2m_min": daily.get("temperature_2m_min") or [999.0] * 7,
    }


def snow_cm_to_inches(cm: float) -> float:
    return max(0.0, float(cm) / 2.54)

def mm_to_inches(mm: float) -> float:
    return max(0.0, float(mm) / 25.4)


def estimate_ice_inches_proxy(precip_mm: float, snow_cm: float, tmin_c: float) -> float:
    """
    Ice proxy:
    - Only when cold enough (<=0C)
    - Only when we’re not already calling it snow-heavy (snow_cm small)
    - Convert precip_mm to inches and multiply by ICE_PROXY_FACTOR
    - Cap to ICE_PROXY_MAX_IN_PER_DAY
    """
    if tmin_c > 0.0:
        return 0.0
    if precip_mm <= 0.0:
        return 0.0

    # If snow is significant, assume that precip is mostly snow (not glaze ice)
    snow_in = snow_cm_to_inches(snow_cm)
    if snow_in >= 1.0:
        return 0.0

    ice_in = mm_to_inches(precip_mm) * ICE_PROXY_FACTOR
    return min(max(0.0, ice_in), ICE_PROXY_MAX_IN_PER_DAY)


def fetch_open_meteo_snow_ice_7d(lat: float, lon: float) -> Tuple[List[float], List[float]]:
    """
    Returns daily_snow_in[7], daily_ice_in[7] based on Open-Meteo.
    Snow: snowfall_sum (cm) -> inches
    Ice: proxy estimate using precip + temp_min
    """
    daily = fetch_open_meteo_daily(lat, lon)

    snow_cm = daily["snowfall_sum"][:7]
    precip_mm = daily["precipitation_sum"][:7]
    tmin_c = daily["temperature_2m_min"][:7]

    daily_snow_in: List[float] = []
    daily_ice_in: List[float] = []

    for i in range(7):
        sc = float(snow_cm[i]) if i < len(snow_cm) else 0.0
        pm = float(precip_mm[i]) if i < len(precip_mm) else 0.0
        tc = float(tmin_c[i]) if i < len(tmin_c) else 999.0

        daily_snow_in.append(snow_cm_to_inches(sc))
        daily_ice_in.append(estimate_ice_inches_proxy(pm, sc, tc))

    while len(daily_snow_in) < 7:
        daily_snow_in.append(0.0)
    while len(daily_ice_in) < 7:
        daily_ice_in.append(0.0)

    return daily_snow_in[:7], daily_ice_in[:7]


# =========================
# CANADA (optional): ECCC ATOM feed headlines
# =========================

def fetch_eccc_atom_alert_titles(feed_url: str) -> List[AlertItem]:
    if not feed_url:
        return []
    xml_text = http_get_text(feed_url, headers={"Accept": "application/atom+xml,application/xml,text/xml"})
    root = ET.fromstring(xml_text)

    ns = {"atom": "http://www.w3.org/2005/Atom"}
    alerts: List[AlertItem] = []

    for entry in root.findall("atom:entry", ns):
        title_el = entry.find("atom:title", ns)
        if title_el is None:
            continue
        title = (title_el.text or "").strip()
        if not title:
            continue

        tl = title.lower()
        if any(k in tl for k in ["warning", "watch", "advisory", "statement", "special weather", "blizzard", "winter storm", "ice storm"]):
            alerts.append(AlertItem(title=title, source="ECCC(ATOM)"))

    seen = set()
    uniq: List[AlertItem] = []
    for a in alerts:
        if a.title in seen:
            continue
        seen.add(a.title)
        uniq.append(a)
    return uniq


# =========================
# REPORTING
# =========================

def fmt_in(x: float) -> str:
    return f"{x:.2f}"

def compute_totals(daily: List[float]) -> float:
    return float(sum(daily))

def render_markdown(results: List[SiteResult]) -> str:
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    flagged = [r for r in results if r.risk_level != "NONE"]
    critical = [r for r in results if r.risk_level == "CRITICAL"]
    warning = [r for r in results if r.risk_level == "WARNING"]
    heads = [r for r in results if r.risk_level == "HEADSUP"]

    def summary_line(r: SiteResult) -> str:
        a = "YES" if r.alerts else "NO"
        return f"- **{r.site_code} — {r.site_name}** ({r.country}) | Alerts: {a} | Snow: {fmt_in(r.snow_7d_in)} in | Ice: {fmt_in(r.ice_7d_in)} in | **{r.risk_level}**"

    md: List[str] = []
    md.append("# Severe Weather Monitor — 7-Day Rolling Outlook\n")
    md.append(f"**Run time:** {now}  ")
    md.append(f"**Timezone label:** {LOCAL_TZ_LABEL}\n")
    md.append(f"**Ice method:** Proxy estimate (subfreezing min temp + precipitation). ICE_PROXY_FACTOR={ICE_PROXY_FACTOR}\n")

    md.append("## Executive Summary\n")
    if not flagged:
        md.append("No sites flagged (no active alerts and forecast accumulation below thresholds).\n")
    else:
        if critical:
            md.append("### Critical (act now)\n")
            for r in sorted(critical, key=lambda x: (-(x.snow_7d_in + x.ice_7d_in), x.site_code)):
                md.append(summary_line(r))
            md.append("")
        if warning:
            md.append("### Warning\n")
            for r in sorted(warning, key=lambda x: (-(x.snow_7d_in + x.ice_7d_in), x.site_code)):
                md.append(summary_line(r))
            md.append("")
        if heads:
            md.append("### Heads-up\n")
            for r in sorted(heads, key=lambda x: (-(x.snow_7d_in + x.ice_7d_in), x.site_code)):
                md.append(summary_line(r))
            md.append("")

    md.append("## Site Table (All)\n")
    md.append("| Site Code | Site | Country | Alerts | Snow (7d, in) | Ice (7d, in) | Risk | Confidence |")
    md.append("|---|---|---:|---:|---:|---:|---:|---:|")
    for r in sorted(results, key=lambda x: x.site_code):
        md.append(
            f"| {r.site_code} | {r.site_name} | {r.country} | "
            f"{'YES' if r.alerts else 'NO'} | {fmt_in(r.snow_7d_in)} | {fmt_in(r.ice_7d_in)} | "
            f"{r.risk_level} | {r.confidence} |"
        )

    if flagged:
        md.append("\n## Details (Flagged Sites)\n")
        for r in sorted(flagged, key=lambda x: (x.risk_level, -(x.snow_7d_in + x.ice_7d_in))):
            md.append(f"### {r.site_code} — {r.site_name} ({r.country})")
            md.append(f"- **Risk:** {r.risk_level} — {r.risk_reason}")
            md.append(f"- **7-day totals:** Snow {fmt_in(r.snow_7d_in)} in | Ice {fmt_in(r.ice_7d_in)} in")
            md.append(f"- **Confidence:** {r.confidence}")
            if r.address:
                md.append(f"- **Address:** {r.address}")

            if r.alerts:
                md.append("\n**Active Alerts:**")
                for a in r.alerts[:10]:
                    parts = [a.title]
                    if a.starts:
                        parts.append(f"start: {a.starts}")
                    if a.ends:
                        parts.append(f"end: {a.ends}")
                    if a.severity:
                        parts.append(f"severity: {a.severity}")
                    parts.append(f"source: {a.source}")
                    md.append("- " + " | ".join(parts))

            md.append("\n**Daily accumulation (next 7 days, UTC buckets):**")
            md.append("- Snow (in): " + ", ".join(fmt_in(x) for x in r.daily_snow_in))
            md.append("- Ice  (in): " + ", ".join(fmt_in(x) for x in r.daily_ice_in))
            md.append("")
    return "\n".join(md).strip() + "\n"


def write_csv(results: List[SiteResult], path: str) -> None:
    fieldnames = [
        "site_code", "site_name", "country", "prime_status", "lat", "lon",
        "risk_level", "risk_reason", "confidence",
        "snow_7d_in", "ice_7d_in",
        "alerts_count", "alerts_titles",
        "daily_snow_in", "daily_ice_in",
        "address",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in results:
            w.writerow({
                "site_code": r.site_code,
                "site_name": r.site_name,
                "country": r.country,
                "prime_status": r.prime_status,
                "lat": r.lat,
                "lon": r.lon,
                "risk_level": r.risk_level,
                "risk_reason": r.risk_reason,
                "confidence": r.confidence,
                "snow_7d_in": round(r.snow_7d_in, 3),
                "ice_7d_in": round(r.ice_7d_in, 3),
                "alerts_count": len(r.alerts),
                "alerts_titles": " || ".join(a.title for a in r.alerts[:10]),
                "daily_snow_in": json.dumps([round(x, 3) for x in r.daily_snow_in]),
                "daily_ice_in": json.dumps([round(x, 3) for x in r.daily_ice_in]),
                "address": r.address,
            })


# =========================
# NOTIFICATIONS (optional)
# =========================

def send_teams(webhook_url: str, title: str, body: str) -> None:
    if not webhook_url:
        return
    payload = {"text": f"**{title}**\n\n{body[:3500]}"}
    r = requests.post(webhook_url, json=payload, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()


def send_email(subject: str, body: str) -> None:
    if not (SMTP_HOST and EMAIL_TO and EMAIL_FROM):
        return

    msg = MIMEMultipart()
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=REQUEST_TIMEOUT) as server:
        server.starttls()
        if SMTP_USER and SMTP_PASS:
            server.login(SMTP_USER, SMTP_PASS)
        recipients = [x.strip() for x in EMAIL_TO.split(",") if x.strip()]
        server.sendmail(EMAIL_FROM, recipients, msg.as_string())


# =========================
# MAIN EVALUATION
# =========================

def evaluate_site(site: Site) -> SiteResult:
    alerts: List[AlertItem] = []
    is_us = site.country.strip().lower() in ["united states", "usa", "us"]
    is_ca = site.country.strip().lower() in ["canada", "ca"]

    # Alerts (best-effort; do not fail site)
    if is_us:
        try:
            alerts = fetch_nws_alerts(site.lat, site.lon)
        except Exception as e:
            alerts = [AlertItem(title=f"(Alert fetch failed) {e}", source="NWS")]
    elif is_ca and site.eccc_feed_url:
        try:
            alerts = fetch_eccc_atom_alert_titles(site.eccc_feed_url)
        except Exception as e:
            alerts = [AlertItem(title=f"(ECCC feed fetch failed) {e}", source="ECCC(ATOM)")]

    # Accumulation (best-effort; do not fail site)
    confidence = "MEDIUM"
    try:
        daily_snow_in, daily_ice_in = fetch_open_meteo_snow_ice_7d(site.lat, site.lon)
    except Exception as e:
        daily_snow_in = [0.0] * 7
        daily_ice_in = [0.0] * 7
        confidence = "LOW"
        # Put the failure into alerts_titles so it's visible but not fatal
        alerts = alerts + [AlertItem(title=f"(Open-Meteo failed) {e}", source="OPEN-METEO")]

    snow_7d = compute_totals(daily_snow_in)
    ice_7d = compute_totals(daily_ice_in)

    has_real_alerts = bool(alerts) and not any("fetch failed" in a.title.lower() for a in alerts)

    risk_level, risk_reason = classify_risk(snow_7d, ice_7d, has_alerts=has_real_alerts)

    return SiteResult(
        site_code=site.site_code,
        site_name=site.site_name,
        country=site.country,
        prime_status=site.prime_status,
        lat=site.lat,
        lon=site.lon,
        alerts=alerts,
        snow_7d_in=snow_7d,
        ice_7d_in=ice_7d,
        daily_snow_in=daily_snow_in,
        daily_ice_in=daily_ice_in,
        risk_level=risk_level,
        risk_reason=risk_reason,
        confidence=confidence,
        address=site.address,
    )


def main() -> int:
    sites = load_sites_from_embedded_csv(SITES_CSV)
    results: List[SiteResult] = []

    for s in sites:
        # evaluate_site is now safe (won't throw in normal API failures)
        results.append(evaluate_site(s))

    md = render_markdown(results)
    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write(md)
    write_csv(results, OUT_CSV)

    flagged = [r for r in results if r.risk_level != "NONE"]
    critical = [r for r in results if r.risk_level == "CRITICAL"]
    warning = [r for r in results if r.risk_level == "WARNING"]
    heads = [r for r in results if r.risk_level == "HEADSUP"]

    subject = f"{EMAIL_SUBJECT_PREFIX}{len(critical)} Critical, {len(warning)} Warning, {len(heads)} Heads-up"

    lines: List[str] = []
    if not flagged:
        lines.append("No sites flagged.")
    else:
        order = {"CRITICAL": 0, "WARNING": 1, "HEADSUP": 2, "NONE": 9}
        top = sorted(flagged, key=lambda x: (order.get(x.risk_level, 9), -(x.snow_7d_in + x.ice_7d_in), x.site_code))[:12]
        for r in top:
            lines.append(
                f"- {r.risk_level}: {r.site_code} {r.site_name} | Snow {fmt_in(r.snow_7d_in)} in | Ice {fmt_in(r.ice_7d_in)} in | Alerts {'YES' if r.alerts else 'NO'}"
            )
    notify_body = "\n".join(lines) + f"\n\n(Full report written to {OUT_MD} and {OUT_CSV}.)\n"

    try:
        if TEAMS_WEBHOOK_URL:
            send_teams(TEAMS_WEBHOOK_URL, subject, notify_body)
    except Exception as e:
        print(f"Teams notification failed: {e}")

    try:
        if SMTP_HOST and EMAIL_TO and EMAIL_FROM:
            send_email(subject, notify_body)
    except Exception as e:
        print(f"Email notification failed: {e}")

    print(f"Wrote {OUT_MD} and {OUT_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
