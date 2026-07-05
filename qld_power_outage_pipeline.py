#!/usr/bin/env python3
from __future__ import annotations

"""
QLD power outage rolling 48-hour pipeline for Power BI.

Purpose
-------
Fetch current Energex + Ergon + Queensland-relevant Essential Energy outage feeds every run, normalise them into a common
schema, assign each outage to a Queensland Local Government Area (LGA), append the
latest outage snapshot to a rolling 48-hour CSV, and build Power BI-friendly LGA
summary tables.

Outputs by default
------------------
data/current_outages.geojson
data/outage_snapshots_48h.csv
data/lga_totals_48h.csv
data/lga_totals_latest.csv
data/lga_cumulative_48h.csv
data/run_manifest.json
data/lga_lookup_cache.json

Design notes
------------
- This script stores one row per active outage per poll. If an outage lasts for
  2 hours and the workflow runs every 15 minutes, it appears in about 8 snapshots.
- "lga_totals_48h.csv" is the main Power BI time-series table.
- "lga_cumulative_48h.csv" estimates customer-hours over the rolling window.
- LGA assignment uses the outage centroid and Queensland Government's ArcGIS
  Local Government area boundary layer. This avoids needing GeoPandas/Shapely.
- Essential Energy is fetched from its KML current-outages feed, then filtered
  to records whose centroid resolves to a Queensland LGA.
- If an outage has no affected customer count, aggregation treats it as 0 but
  keeps counters so you can see how many unknown-count outages exist.

Example
-------
python scripts/qld_power_outage_pipeline.py --output-dir data --debug
"""

import argparse
import csv
import hashlib
import html
import json
import math
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple
import xml.etree.ElementTree as ET
from urllib.parse import urlparse

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - Python <3.9 fallback not expected in GitHub Actions
    ZoneInfo = None  # type: ignore

import requests


# --------------------------------------------------------------------------------------
# Source endpoints
# --------------------------------------------------------------------------------------

ENERGEX_URLS = [
    # Current unplanned
    "https://www.energex.com.au/api/outages-map/filestore-404/"
    "ex-map-current-unplanned/filestore-proxy?SQ_SESSION_MODE=read_only_async",
    # Current planned
    "https://www.energex.com.au/api/outages-map/filestore-404/"
    "ex-map-current-planned/filestore-proxy?SQ_SESSION_MODE=read_only_async",
]

ENERGEX_OUTAGE_MAP_URL = "https://www.energex.com.au/outages/outage-finder/outage-finder-map/"

ERGON_LAYER_URL = (
    "https://services.arcgis.com/33eHbTVqo7gtiCE8/"
    "arcgis/rest/services/VwErgonOutages/FeatureServer/0"
)

ESSENTIAL_ENERGY_KML_URL = "https://www.essentialenergy.com.au/Assets/kmz/current.kml?dFdLgoAP"
ESSENTIAL_PROVIDER_CODE = "au.qld.essentialenergy"
ESSENTIAL_PROVIDER_NAME = "Essential Energy"

QLD_LGA_LAYER_URL = (
    "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/"
    "Boundaries/AdminBoundariesFramework/FeatureServer/11"
)

# Fallback Queensland inclusion polygon supplied from the user's previous Power Query filter.
# This is used only as a fast Essential Energy pre-filter / fallback; final LGA assignment
# still uses the official QLD LGA layer above. Coordinates are KML lon,lat,alt tokens.
QLD_FALLBACK_POLYGON_TEXT = """153.9237537997431,-23.34560511810547,0 142.4635861406184,-9.969105835014322,0 137.9783459734518,-15.49091200537405,0 137.9965519785626,-25.99728871475233,0 140.9960944168225,-26.0186431391937,0 140.9921944675683,-28.99489777926846,0 148.9545057053679,-28.99840499720988,0 149.0231435189979,-28.97957580503295,0 149.087536094034,-28.85797306288737,0 149.2113655965773,-28.77444290051769,0 149.3906039832582,-28.70136653140777,0 149.4673191465016,-28.60923681910268,0 149.5703046765741,-28.59198741289737,0 149.6769843324777,-28.64651225137474,0 150.210555716046,-28.57608022516586,0 150.2918261862552,-28.54704027101757,0 150.4513270322157,-28.68068460775324,0 150.6124877274127,-28.6779534702994,0 150.7493818808105,-28.64193404274211,0 150.8561987954816,-28.69826892448033,0 150.8920497278235,-28.70497841857001,0 150.9075716015604,-28.70307448298706,0 150.944998086894,-28.74012105541771,0 151.0095258134703,-28.76829041741151,0 151.0547475266175,-28.85435138190378,0 151.0978057898163,-28.84625202825183,0 151.273998061244,-28.9577084617204,0 151.276302620664,-29.09101612335235,0 151.3254298579594,-29.17975134439713,0 151.4015681427035,-29.17834881366439,0 151.5065664427469,-29.08140523117263,0 151.5074321169996,-29.0285555785973,0 151.5690844105399,-28.95947124895127,0 151.6820515850914,-28.91408863278213,0 151.7302199751953,-28.87984664121307,0 151.7859107405384,-28.96727743439638,0 151.8328940498949,-28.96405640268005,0 151.8580161528315,-28.92481548154452,0 151.9156722139599,-28.92857463807395,0 152.0219323658982,-28.92029912912725,0 152.0441289108853,-28.86452078590606,0 152.0487346409794,-28.81678758029678,0 152.0589037873251,-28.74605875881931,0 152.0910014995858,-28.71142905814722,0 152.0673492655374,-28.68390268994736,0 152.0083728472828,-28.65268680768453,0 151.9920150447213,-28.57431952492789,0 151.9701673845502,-28.538298093025,0 151.9872398285485,-28.50840646994336,0 152.0293063004834,-28.53348036154076,0 152.1688392151868,-28.44354421101708,0 152.2241604735467,-28.45306015764548,0 152.3214053726011,-28.37637564000451,0 152.3987141017151,-28.3687797482925,0 152.4529062132822,-28.29792721583617,0 152.472707796306,-28.26744897040317,0 152.5329877272242,-28.26799555905704,0 152.5272790209334,-28.31008037774886,0 152.5921652712647,-28.33514416546279,0 152.6171734971932,-28.2790751818363,0 152.6423392233303,-28.31460738008539,0 152.759915887051,-28.37121518552279,0 152.8692663684246,-28.32607241599176,0 153.1252668166022,-28.358091281931,0 153.1861289486507,-28.25520012252661,0 153.2389102750371,-28.26477924515282,0 153.2929894852641,-28.24631666112539,0 153.3489850492703,-28.25543903235208,0 153.3837986335487,-28.25349366020756,0 153.4397685247268,-28.19287247654148,0 153.4663040070358,-28.18084675380926,0 153.4902678510976,-28.15521344217365,0 153.5217379064742,-28.17039062249907,0 153.5689104881603,-28.16658240510656,0 153.9237537997431,-23.34560511810547,0"""

HTTP_RETRIES = 3
RETRY_SLEEP_SECONDS = 1.5

SNAPSHOT_COLUMNS = [
    "snapshot_utc",
    "snapshot_aest",
    "provider_code",
    "provider_name",
    "outage_key",
    "outage_id",
    "outage_type",
    "status",
    "reason",
    "affected_customers",
    "affected_customers_known",
    "suburb",
    "street_name",
    "start_utc",
    "etr_utc",
    "centroid_lon",
    "centroid_lat",
    "lga_name",
    "lga_code",
    "source_url",
    "geometry_type",
    "geometry_json",
    "raw_json",
]

LGA_TOTAL_COLUMNS = [
    "snapshot_utc",
    "snapshot_aest",
    "lga_name",
    "lga_code",
    "provider_code",
    "provider_name",
    "active_outage_count",
    "known_customer_count_outage_count",
    "unknown_customer_count_outage_count",
    "total_affected_customers",
    "planned_customers",
    "unplanned_customers",
    "unknown_type_customers",
]

CUMULATIVE_COLUMNS = [
    "window_start_utc",
    "window_end_utc",
    "lga_name",
    "lga_code",
    "provider_code",
    "provider_name",
    "snapshot_count",
    "latest_snapshot_utc",
    "latest_affected_customers",
    "max_affected_customers",
    "customer_snapshot_sum_48h",
    "estimated_customer_minutes_48h",
    "estimated_customer_hours_48h",
    "estimated_customer_days_48h",
]


# --------------------------------------------------------------------------------------
# Small conversion helpers
# --------------------------------------------------------------------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def to_utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_now_iso() -> str:
    return to_utc_iso(utc_now())


def to_aest_iso(dt: datetime) -> str:
    if ZoneInfo is not None:
        return dt.astimezone(ZoneInfo("Australia/Brisbane")).replace(microsecond=0).isoformat()
    # Brisbane has no daylight savings, so UTC+10 is stable.
    return (dt.astimezone(timezone.utc) + timedelta(hours=10)).replace(microsecond=0).isoformat().replace("+00:00", "+10:00")


def parse_utc_iso(s: Any) -> Optional[datetime]:
    if not s:
        return None
    try:
        text = str(s).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def as_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = x.strip() if isinstance(x, str) else str(x).strip()
    return s if s else None


def as_int(x: Any) -> Optional[int]:
    try:
        if x is None or x == "":
            return None
        if isinstance(x, bool):
            return int(x)
        if isinstance(x, int):
            return x
        if isinstance(x, float):
            if math.isnan(x):
                return None
            return int(round(x))
        s = str(x).strip().replace(",", "")
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def parse_epoch_any_to_utc_iso(x: Any) -> Optional[str]:
    """
    Best-effort parsing:
    - epoch ms / epoch seconds
    - numeric strings
    - ISO8601 strings

    Some feeds include human-readable local strings. Those are retained in raw_json
    but not parsed here unless they are ISO-like.
    """
    if x is None or x == "":
        return None

    if isinstance(x, (int, float)):
        try:
            v = float(x)
            dt = datetime.fromtimestamp(v / 1000.0, tz=timezone.utc) if v > 1e12 else datetime.fromtimestamp(v, tz=timezone.utc)
            return to_utc_iso(dt)
        except Exception:
            return None

    s = str(x).strip()
    if not s:
        return None

    # numeric string
    try:
        if s.isdigit():
            return parse_epoch_any_to_utc_iso(int(s))
        # Handles "1719459123.0"
        float(s)
        return parse_epoch_any_to_utc_iso(float(s))
    except Exception:
        pass

    try:
        s2 = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return to_utc_iso(dt)
    except Exception:
        return None


def first_present(d: Dict[str, Any], keys: Iterable[str]) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, "", [], {}):
            return d[k]
    return None


def lower_or_none(x: Any) -> Optional[str]:
    s = as_str(x)
    return s.lower() if s else None


def json_dumps_compact(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


# --------------------------------------------------------------------------------------
# HTTP / ArcGIS
# --------------------------------------------------------------------------------------

def browser_like_headers(
    url: str,
    user_agent: str,
    accept: str = "application/json,*/*",
    referer: Optional[str] = None,
    include_origin: bool = False,
) -> Dict[str, str]:
    """
    Some outage endpoints reject script-looking requests but allow ordinary browser-style
    requests. Keep this helper conservative and transparent: no auth bypassing, just
    normal browser request metadata.
    """
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else ""
    headers = {
        "User-Agent": user_agent,
        "Accept": accept,
        "Accept-Language": "en-AU,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
    }
    if referer:
        headers["Referer"] = referer
    elif origin:
        headers["Referer"] = origin + "/"
    if include_origin and origin:
        headers["Origin"] = origin
    return headers


def is_energex_url(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower()
        return host.endswith("energex.com.au")
    except Exception:
        return False


def energex_headers(user_agent: str) -> Dict[str, str]:
    """Headers that mimic the browser request made from the Energex outage map."""
    return {
        "User-Agent": user_agent,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-AU,en;q=0.9",
        "Referer": ENERGEX_OUTAGE_MAP_URL,
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }


def http_get_json_curl_impersonate(url: str, timeout: int, user_agent: str) -> Any:
    """
    Energex can reject GitHub-hosted Python/requests traffic with 403 even when the
    same public endpoint works in a browser/local run. As a last resort, use
    curl_cffi's browser TLS impersonation. If curl_cffi is not installed, raise a
    clear error so the manifest explains the missing dependency.
    """
    try:
        from curl_cffi import requests as curl_requests  # type: ignore
    except Exception as e:  # pragma: no cover - depends on optional dependency
        raise RuntimeError(
            "curl_cffi is not installed; install requirements.txt so Energex "
            "GitHub Actions fallback can run"
        ) from e

    browser_ua = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    ua = user_agent if user_agent and "Mozilla/" in user_agent else browser_ua
    headers = energex_headers(ua)

    # Prime the same-origin session with the map page so any public site cookies are set.
    session = curl_requests.Session()
    try:
        session.get(
            ENERGEX_OUTAGE_MAP_URL,
            headers={
                "User-Agent": ua,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-AU,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            },
            timeout=timeout,
            impersonate="chrome",
        )
    except Exception:
        # Priming is helpful but not mandatory; continue to the JSON request.
        pass

    r = session.get(url, headers=headers, timeout=timeout, impersonate="chrome")
    if r.status_code in (401, 403, 429, 503):
        raise requests.HTTPError(f"{r.status_code} response for {url} using curl_cffi fallback", response=r)
    r.raise_for_status()
    return r.json()


def http_get_json(url: str, timeout: int, user_agent: str) -> Any:
    """
    Fetch JSON with retries. For sites such as Energex that may return 403 to
    GitHub-hosted Python requests, try increasingly browser-like request variants,
    then an optional curl_cffi Chrome/TLS impersonation fallback.
    """
    browser_ua = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )

    if is_energex_url(url):
        header_variants = [
            energex_headers(browser_ua),
            browser_like_headers(
                url,
                browser_ua,
                accept="application/json, text/plain, */*",
                referer=ENERGEX_OUTAGE_MAP_URL,
                include_origin=False,
            ),
            browser_like_headers(
                url,
                user_agent,
                accept="application/json, text/plain, */*",
                referer=ENERGEX_OUTAGE_MAP_URL,
                include_origin=False,
            ),
        ]
        prime_url = ENERGEX_OUTAGE_MAP_URL
    else:
        header_variants = [
            browser_like_headers(url, user_agent, accept="application/json,text/plain,*/*", include_origin=False),
            browser_like_headers(url, browser_ua, accept="application/json,text/plain,*/*", include_origin=False),
        ]
        prime_url = None

    last_err: Optional[Exception] = None
    with requests.Session() as s:
        # For Energex, first visit the public outage map page to pick up any public cookies.
        if prime_url:
            try:
                s.get(
                    prime_url,
                    timeout=timeout,
                    headers=browser_like_headers(
                        prime_url,
                        browser_ua,
                        accept="text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        referer="https://www.energex.com.au/",
                        include_origin=False,
                    ),
                )
            except Exception:
                pass

        for attempt in range(1, HTTP_RETRIES + 1):
            for headers in header_variants:
                try:
                    r = s.get(url, timeout=timeout, headers=headers)
                    # If one header variant is blocked, immediately try the next
                    # variant before sleeping/retrying.
                    if r.status_code in (401, 403, 429, 503):
                        last_err = requests.HTTPError(f"{r.status_code} response for {url}", response=r)
                        continue
                    r.raise_for_status()
                    return r.json()
                except Exception as e:
                    last_err = e
            if attempt < HTTP_RETRIES:
                time.sleep(RETRY_SLEEP_SECONDS * attempt)

    if is_energex_url(url):
        try:
            return http_get_json_curl_impersonate(url, timeout=timeout, user_agent=user_agent)
        except Exception as e:
            last_err = e

    if last_err:
        raise last_err
    raise RuntimeError(f"Failed to fetch JSON: {url}")  # pragma: no cover

def http_get_text(url: str, timeout: int, user_agent: str) -> str:
    last_err: Optional[Exception] = None
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/vnd.google-earth.kml+xml, application/xml, text/xml, */*",
        "Accept-Language": "en-AU,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://www.essentialenergy.com.au/",
    }
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = requests.get(url, timeout=timeout, headers=headers)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            if attempt < HTTP_RETRIES:
                time.sleep(RETRY_SLEEP_SECONDS * attempt)
            else:
                raise
    raise RuntimeError(last_err)  # pragma: no cover


def arcgis_query(layer_url: str, params: Dict[str, Any], timeout: int, user_agent: str) -> Dict[str, Any]:
    """
    ArcGIS query helper.

    Uses POST for long requests/objectIds to avoid URL length issues. Also tries both
    /arcgis/rest and /ArcGIS/rest variants because some ArcGIS front doors are picky.
    """
    def _do_request(base_url: str) -> requests.Response:
        url = base_url.rstrip("/") + "/query"
        headers = {
            "User-Agent": user_agent,
            "Accept": "application/json,*/*",
            "Referer": base_url,
        }
        use_post = "objectIds" in params or len(str(params)) > 1500
        if use_post:
            return requests.post(url, data=params, timeout=timeout, headers=headers)
        return requests.get(url, params=params, timeout=timeout, headers=headers)

    candidates = [layer_url]
    if "/arcgis/rest/" in layer_url:
        candidates.append(layer_url.replace("/arcgis/rest/", "/ArcGIS/rest/"))
    elif "/ArcGIS/rest/" in layer_url:
        candidates.append(layer_url.replace("/ArcGIS/rest/", "/arcgis/rest/"))

    last_exc: Optional[Exception] = None
    for attempt in range(1, HTTP_RETRIES + 1):
        for base in candidates:
            try:
                r = _do_request(base)
                if r.status_code == 404 and len(candidates) > 1:
                    continue
                r.raise_for_status()
                j = r.json()
                if isinstance(j, dict) and j.get("error"):
                    raise RuntimeError(f"ArcGIS error: {j['error']}")
                if not isinstance(j, dict):
                    raise RuntimeError("ArcGIS response is not a JSON object")
                return j
            except Exception as e:
                last_exc = e

        if attempt < HTTP_RETRIES:
            time.sleep(RETRY_SLEEP_SECONDS * attempt)

    raise RuntimeError(f"ArcGIS query failed after retries. Last error: {last_exc}")


def arcgis_get_object_ids(layer_url: str, where: str, timeout: int, user_agent: str) -> Tuple[List[int], str]:
    j = arcgis_query(
        layer_url,
        {"where": where, "returnIdsOnly": "true", "f": "json"},
        timeout=timeout,
        user_agent=user_agent,
    )
    oids = j.get("objectIds") or []
    out: List[int] = []
    if isinstance(oids, list):
        for x in oids:
            try:
                out.append(int(x))
            except Exception:
                pass

    oid_field = j.get("objectIdFieldName")
    if not isinstance(oid_field, str) or not oid_field.strip():
        oid_field = "OBJECTID"

    return sorted(set(out)), oid_field


def arcgis_fetch_features_by_object_ids(
    layer_url: str,
    object_ids: List[int],
    where: str,
    timeout: int,
    chunk_size: int,
    user_agent: str,
) -> List[Dict[str, Any]]:
    feats: List[Dict[str, Any]] = []
    if not object_ids:
        return feats

    total = len(object_ids)
    chunks = int(math.ceil(total / float(chunk_size)))

    for i in range(chunks):
        sub = object_ids[i * chunk_size : (i + 1) * chunk_size]
        params = {
            "where": where,
            "objectIds": ",".join(str(x) for x in sub),
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "json",
        }
        j = arcgis_query(layer_url, params, timeout=timeout, user_agent=user_agent)
        rows = j.get("features") or []
        if isinstance(rows, list):
            for r in rows:
                if isinstance(r, dict) and isinstance(r.get("attributes"), dict):
                    feats.append(r)

    return feats


# --------------------------------------------------------------------------------------
# Geometry helpers
# --------------------------------------------------------------------------------------

def extract_point_and_best_geom(geom: Any) -> Tuple[Optional[Tuple[float, float]], Optional[Dict[str, Any]]]:
    """
    Energex geometry is often:
      {"type":"GeometryCollection","geometries":[{"type":"Point",...},{"type":"Polygon",...}]}

    Returns:
      (point_lon_lat, best_geometry_geojson)

    best_geometry prefers Polygon/MultiPolygon, then Point/other.
    """
    if not isinstance(geom, dict) or not geom:
        return None, None

    gtype = geom.get("type")

    if gtype in ("Polygon", "MultiPolygon", "Point", "MultiPoint", "LineString", "MultiLineString"):
        pt = None
        if gtype == "Point":
            coords = geom.get("coordinates")
            if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                try:
                    pt = (float(coords[0]), float(coords[1]))
                except Exception:
                    pt = None
        return pt, geom

    if gtype != "GeometryCollection":
        return None, None

    geoms = geom.get("geometries") or []
    if not isinstance(geoms, list):
        return None, None

    pt: Optional[Tuple[float, float]] = None
    poly: Optional[Dict[str, Any]] = None
    fallback: Optional[Dict[str, Any]] = None

    for g in geoms:
        if not isinstance(g, dict):
            continue
        t = g.get("type")

        if t == "Point" and pt is None:
            coords = g.get("coordinates")
            if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                try:
                    pt = (float(coords[0]), float(coords[1]))
                except Exception:
                    pt = None
            fallback = fallback or g

        elif t in ("Polygon", "MultiPolygon") and poly is None:
            poly = g

        elif fallback is None and t in ("Point", "MultiPoint", "LineString", "MultiLineString"):
            fallback = g

    return pt, poly or fallback


def esri_geometry_to_geojson(g: Any) -> Optional[Dict[str, Any]]:
    """
    Converts common Esri JSON geometries to GeoJSON.
    Supports Point (x/y), Polygon (rings), Line (paths), MultiPoint (points).
    """
    if not isinstance(g, dict) or not g:
        return None

    if "x" in g and "y" in g:
        try:
            return {"type": "Point", "coordinates": [float(g["x"]), float(g["y"])]}
        except Exception:
            return None

    if "rings" in g:
        rings = g.get("rings")
        if not isinstance(rings, list) or not rings:
            return None
        out_rings: List[List[List[float]]] = []
        for ring in rings:
            if not isinstance(ring, list):
                continue
            coords: List[List[float]] = []
            for pt in ring:
                if isinstance(pt, (list, tuple)) and len(pt) >= 2:
                    try:
                        coords.append([float(pt[0]), float(pt[1])])
                    except Exception:
                        pass
            if len(coords) >= 3:
                if coords[0] != coords[-1]:
                    coords.append(coords[0])
                out_rings.append(coords)
        if not out_rings:
            return None
        return {"type": "Polygon", "coordinates": out_rings}

    if "paths" in g:
        paths = g.get("paths")
        if not isinstance(paths, list) or not paths:
            return None
        lines: List[List[List[float]]] = []
        for path in paths:
            if not isinstance(path, list):
                continue
            coords: List[List[float]] = []
            for pt in path:
                if isinstance(pt, (list, tuple)) and len(pt) >= 2:
                    try:
                        coords.append([float(pt[0]), float(pt[1])])
                    except Exception:
                        pass
            if len(coords) >= 2:
                lines.append(coords)
        if not lines:
            return None
        if len(lines) == 1:
            return {"type": "LineString", "coordinates": lines[0]}
        return {"type": "MultiLineString", "coordinates": lines}

    if "points" in g:
        pts = g.get("points")
        if not isinstance(pts, list) or not pts:
            return None
        coords: List[List[float]] = []
        for pt in pts:
            if isinstance(pt, (list, tuple)) and len(pt) >= 2:
                try:
                    coords.append([float(pt[0]), float(pt[1])])
                except Exception:
                    pass
        if not coords:
            return None
        return {"type": "MultiPoint", "coordinates": coords}

    return None


def iter_coords(obj: Any) -> Iterator[Tuple[float, float]]:
    if isinstance(obj, (list, tuple)):
        if len(obj) >= 2 and isinstance(obj[0], (int, float)) and isinstance(obj[1], (int, float)):
            yield float(obj[0]), float(obj[1])
        else:
            for item in obj:
                yield from iter_coords(item)


def ring_area_and_centroid(ring: Sequence[Sequence[float]]) -> Tuple[float, Optional[Tuple[float, float]]]:
    """
    Planar centroid for a single closed ring in lon/lat coordinates.
    Good enough for small outage polygons and LGA point assignment.
    """
    pts: List[Tuple[float, float]] = []
    for p in ring:
        if isinstance(p, (list, tuple)) and len(p) >= 2:
            try:
                pts.append((float(p[0]), float(p[1])))
            except Exception:
                pass

    if len(pts) < 3:
        return 0.0, None
    if pts[0] != pts[-1]:
        pts.append(pts[0])

    area2 = 0.0
    cx6a = 0.0
    cy6a = 0.0

    for (x1, y1), (x2, y2) in zip(pts, pts[1:]):
        cross = x1 * y2 - x2 * y1
        area2 += cross
        cx6a += (x1 + x2) * cross
        cy6a += (y1 + y2) * cross

    if abs(area2) < 1e-15:
        return 0.0, None

    area = area2 / 2.0
    cx = cx6a / (3.0 * area2)
    cy = cy6a / (3.0 * area2)
    return area, (cx, cy)


def geometry_centroid(geom: Optional[Dict[str, Any]]) -> Tuple[Optional[float], Optional[float]]:
    """
    Returns (lon, lat). Uses:
    - Point coordinate directly
    - Polygon outer-ring centroid
    - MultiPolygon largest outer-ring centroid
    - fallback average coordinate for lines/multipoints/odd geometry
    """
    if not isinstance(geom, dict):
        return None, None

    gtype = geom.get("type")
    coords = geom.get("coordinates")

    if gtype == "GeometryCollection":
        sub_geoms = geom.get("geometries") or []
        if isinstance(sub_geoms, list):
            centroids: List[Tuple[float, float]] = []
            for sub_geom in sub_geoms:
                lon, lat = geometry_centroid(sub_geom if isinstance(sub_geom, dict) else None)
                if lon is not None and lat is not None:
                    centroids.append((lon, lat))
            if centroids:
                return (
                    sum(lon for lon, _ in centroids) / len(centroids),
                    sum(lat for _, lat in centroids) / len(centroids),
                )
        return None, None

    if gtype == "Point" and isinstance(coords, (list, tuple)) and len(coords) >= 2:
        try:
            return float(coords[0]), float(coords[1])
        except Exception:
            return None, None

    if gtype == "Polygon" and isinstance(coords, list) and coords:
        outer = coords[0]
        if isinstance(outer, list):
            area, cen = ring_area_and_centroid(outer)
            if cen:
                return cen
        pts = list(iter_coords(coords))
        if pts:
            return sum(x for x, _ in pts) / len(pts), sum(y for _, y in pts) / len(pts)

    if gtype == "MultiPolygon" and isinstance(coords, list) and coords:
        best_area = 0.0
        best_cen: Optional[Tuple[float, float]] = None
        for poly in coords:
            if isinstance(poly, list) and poly:
                outer = poly[0]
                if isinstance(outer, list):
                    area, cen = ring_area_and_centroid(outer)
                    if cen and abs(area) > best_area:
                        best_area = abs(area)
                        best_cen = cen
        if best_cen:
            return best_cen
        pts = list(iter_coords(coords))
        if pts:
            return sum(x for x, _ in pts) / len(pts), sum(y for _, y in pts) / len(pts)

    pts = list(iter_coords(coords))
    if pts:
        return sum(x for x, _ in pts) / len(pts), sum(y for _, y in pts) / len(pts)

    return None, None


# --------------------------------------------------------------------------------------
# Normalised record
# --------------------------------------------------------------------------------------

@dataclass
class OutageRecord:
    provider_code: str
    provider_name: str
    outage_key: str
    outage_id: Optional[str]
    outage_type: Optional[str]
    status: Optional[str]
    reason: Optional[str]
    affected_customers: Optional[int]
    suburb: Optional[str]
    street_name: Optional[str]
    start_utc: Optional[str]
    etr_utc: Optional[str]
    centroid_lon: Optional[float]
    centroid_lat: Optional[float]
    source_url: str
    geometry: Optional[Dict[str, Any]]
    raw: Dict[str, Any]
    lga_name: Optional[str] = None
    lga_code: Optional[str] = None

    @property
    def affected_customers_known(self) -> bool:
        return self.affected_customers is not None


def make_outage_key(
    provider_code: str,
    outage_id: Optional[str],
    outage_type: Optional[str],
    centroid_lon: Optional[float],
    centroid_lat: Optional[float],
    start_utc: Optional[str],
    suburb: Optional[str],
    raw: Dict[str, Any],
) -> str:
    """
    Prefer provider IDs when available; otherwise create a stable-ish hash from
    fields that should remain consistent across runs.
    """
    clean_type = outage_type or "unknown"

    if outage_id:
        return f"{provider_code}|{clean_type}|{outage_id}"

    seed = {
        "provider_code": provider_code,
        "outage_type": clean_type,
        "centroid_lon": round(float(centroid_lon), 5) if centroid_lon is not None else None,
        "centroid_lat": round(float(centroid_lat), 5) if centroid_lat is not None else None,
        "start_utc": start_utc,
        "suburb": suburb,
        # Add a small raw-property digest to avoid merging unrelated no-ID outages.
        "raw_digest_basis": raw,
    }
    digest = hashlib.sha1(json_dumps_compact(seed).encode("utf-8")).hexdigest()[:16]
    return f"{provider_code}|{clean_type}|hash:{digest}"


def extract_customers_affected(props: Dict[str, Any]) -> Optional[int]:
    for k in (
        "CUSTOMERS_AFFECTED",
        "CUSTOMERS",
        "CUSTOMER_COUNT",
        "CUSTOMERS_OUT",
        "AFFECTED_CUSTOMERS",
        "NO_CUSTOMERS",
        "NUM_CUSTOMERS",
        "CUSTOMERSAFFECTED",
        "customersAffected",
        "numberCustomerAffected",
        "numberCustomersAffected",
        "CustomersAffected",
        "customers_affected",
        "CustomerCount",
    ):
        if k in props and props[k] is not None:
            n = as_int(props[k])
            if n is not None and n >= 0:
                return n
    return None


def infer_outage_type(attrs: Dict[str, Any], source_hint: Optional[str] = None) -> Optional[str]:
    if source_hint:
        source_hint_l = source_hint.lower()
        if "unplanned" in source_hint_l or "unscheduled" in source_hint_l:
            return "unplanned"
        if "planned" in source_hint_l or "scheduled" in source_hint_l:
            return "planned"

    for k in (
        "TYPE",
        "type",
        "OUTAGE_TYPE",
        "outage_type",
        "CATEGORY",
        "category",
        "PLANNED",
        "planned",
        "OUTAGETYPE",
        "outageType",
    ):
        if k in attrs and attrs[k] is not None:
            v = str(attrs[k]).strip().lower()
            if v in ("planned", "schedule", "scheduled", "true", "1", "yes"):
                return "planned"
            if v in ("unplanned", "unscheduled", "un-scheduled", "false", "0", "no"):
                return "unplanned"
            if "unplanned" in v or "unscheduled" in v:
                return "unplanned"
            if "planned" in v or "scheduled" in v:
                return "planned"

    saw_unplanned = False
    saw_planned = False
    for v in attrs.values():
        if v is None:
            continue
        s = str(v).lower()
        if "unplanned" in s or "unscheduled" in s:
            saw_unplanned = True
        # Check planned second; 'unplanned' contains 'planned'.
        if ("planned" in s or "scheduled" in s) and ("unplanned" not in s and "unscheduled" not in s):
            saw_planned = True

    if saw_planned and not saw_unplanned:
        return "planned"
    if saw_unplanned and not saw_planned:
        return "unplanned"
    return None


# --------------------------------------------------------------------------------------
# Essential Energy KML helpers
# --------------------------------------------------------------------------------------

def strip_html_tags(value: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", value, flags=re.I)
    text = re.sub(r"</p\s*>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    return html.unescape(text).strip()


def parse_kml_coordinates(coord_text: str) -> List[List[float]]:
    coords: List[List[float]] = []
    for part in re.split(r"\s+", coord_text.strip()):
        if not part:
            continue
        bits = part.split(",")
        if len(bits) < 2:
            continue
        try:
            coords.append([float(bits[0]), float(bits[1])])
        except Exception:
            continue
    return coords


def ensure_closed_ring(points: List[List[float]]) -> List[List[float]]:
    if not points:
        return []
    out = [list(p) for p in points]
    if out[0] != out[-1]:
        out.append(out[0])
    return out


def point_on_segment(lon: float, lat: float, lon1: float, lat1: float, lon2: float, lat2: float, eps: float = 1e-10) -> bool:
    cross = (lon2 - lon1) * (lat - lat1) - (lat2 - lat1) * (lon - lon1)
    if abs(cross) > eps:
        return False
    return (
        min(lon1, lon2) - eps <= lon <= max(lon1, lon2) + eps
        and min(lat1, lat2) - eps <= lat <= max(lat1, lat2) + eps
    )


def point_in_polygon(lon: Optional[float], lat: Optional[float], polygon_lon_lat: Sequence[Sequence[float]]) -> bool:
    """Ray-casting point-in-polygon. Boundary points count as inside."""
    if lon is None or lat is None or len(polygon_lon_lat) < 3:
        return False

    x = float(lon)
    y = float(lat)
    inside = False
    pts = list(polygon_lon_lat)
    n = len(pts)

    for i in range(n):
        j = n - 1 if i == 0 else i - 1
        try:
            xi, yi = float(pts[i][0]), float(pts[i][1])
            xj, yj = float(pts[j][0]), float(pts[j][1])
        except Exception:
            continue

        if point_on_segment(x, y, xi, yi, xj, yj):
            return True

        intersects = ((yi > y) != (yj > y))
        if intersects:
            x_intersect = xi if abs(yj - yi) < 1e-15 else xi + (y - yi) * (xj - xi) / (yj - yi)
            if x < x_intersect:
                inside = not inside

    return inside


QLD_FALLBACK_POLYGON = ensure_closed_ring(parse_kml_coordinates(QLD_FALLBACK_POLYGON_TEXT))


def point_in_qld_fallback_polygon(lon: Optional[float], lat: Optional[float]) -> bool:
    return point_in_polygon(lon, lat, QLD_FALLBACK_POLYGON)


def decode_essential_html(value: Optional[str]) -> str:
    """Decode Essential Energy's occasionally double/triple-escaped KML description HTML."""
    if not value:
        return ""
    text = str(value)
    # Handle the exact Power Query replacements first, then repeatedly HTML-unescape until stable.
    text = text.replace("&amp;amp;lt;", "&amp;lt;").replace("&amp;amp;gt;", "&amp;gt;").replace("&amp;amp;amp;", "&amp;amp;")
    for _ in range(5):
        new_text = html.unescape(text)
        if new_text == text:
            break
        text = new_text
    return text.strip()


def text_between(value: str, start: str, end: str) -> Optional[str]:
    if not value:
        return None
    try:
        i = value.index(start) + len(start)
        j = value.index(end, i)
        out = value[i:j].strip()
        return out if out else None
    except ValueError:
        return None


def essential_label_value(decoded_html: str, label: str) -> Optional[str]:
    """Extract values after labels like <span>Time Off:</span>... </div>."""
    if not decoded_html:
        return None
    # Essential's KML currently uses <span>Label:</span>value</div>, but allow attributes/spacing.
    pattern = re.compile(
        r"<span[^>]*>\s*" + re.escape(label) + r"\s*</span>\s*(.*?)\s*</div>",
        flags=re.I | re.S,
    )
    m = pattern.search(decoded_html)
    if not m:
        return None
    value = strip_html_tags(m.group(1))
    return as_str(value)


def parse_au_local_datetime_to_utc_iso(value: Any, timezone_name: str = "Australia/Sydney") -> Optional[str]:
    """Parse common Australian local date/time strings to UTC ISO.

    Essential Energy's KML fields are human-readable and normally do not include an
    explicit timezone. The default is Australia/Sydney because Essential Energy is NSW-based.
    This only affects informational start/restore fields; the rolling 48h calculations use
    snapshot times.
    """
    raw = as_str(value)
    if not raw:
        return None

    # Try already-supported epoch/ISO first.
    iso = parse_epoch_any_to_utc_iso(raw)
    if iso:
        return iso

    text = re.sub(r"\s+", " ", raw).strip()
    text = text.replace("a.m.", "AM").replace("p.m.", "PM").replace("am", "AM").replace("pm", "PM")

    formats = [
        "%d/%m/%Y %I:%M:%S %p",
        "%d/%m/%Y %I:%M %p",
        "%d/%m/%y %I:%M:%S %p",
        "%d/%m/%y %I:%M %p",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%y %H:%M:%S",
        "%d/%m/%y %H:%M",
        "%d %b %Y %I:%M %p",
        "%d %b %Y %H:%M",
        "%I:%M%p %d %b %Y",
        "%I:%M %p %d %b %Y",
        "%H:%M %d %b %Y",
    ]

    tz = timezone.utc
    if ZoneInfo is not None:
        try:
            tz = ZoneInfo(timezone_name)
        except Exception:
            tz = timezone.utc

    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt)
            dt = dt.replace(tzinfo=tz)
            return to_utc_iso(dt)
        except Exception:
            pass

    return None


def find_kml_text(el: ET.Element, path: str, ns: Dict[str, str]) -> Optional[str]:
    node = el.find(path, ns)
    if node is None or node.text is None:
        return None
    text = node.text.strip()
    return text if text else None


def kml_placemark_geometry(pm: ET.Element, ns: Dict[str, str]) -> Optional[Dict[str, Any]]:
    # Prefer polygons when present, then points, then lines. MultiGeometry becomes a GeometryCollection.
    geoms: List[Dict[str, Any]] = []

    for poly in pm.findall(".//kml:Polygon", ns):
        ring = poly.find(".//kml:outerBoundaryIs/kml:LinearRing/kml:coordinates", ns)
        if ring is not None and ring.text:
            coords = parse_kml_coordinates(ring.text)
            if coords:
                if coords[0] != coords[-1]:
                    coords.append(coords[0])
                geoms.append({"type": "Polygon", "coordinates": [coords]})

    for pt in pm.findall(".//kml:Point/kml:coordinates", ns):
        if pt is not None and pt.text:
            coords = parse_kml_coordinates(pt.text)
            if coords:
                geoms.append({"type": "Point", "coordinates": coords[0]})

    for ls in pm.findall(".//kml:LineString/kml:coordinates", ns):
        if ls is not None and ls.text:
            coords = parse_kml_coordinates(ls.text)
            if coords:
                geoms.append({"type": "LineString", "coordinates": coords})

    if not geoms:
        return None
    if len(geoms) == 1:
        return geoms[0]

    # If there are multiple geometries and at least one polygon, use the first polygon for LGA matching/reporting.
    # Keeping a GeometryCollection can be useful, but Power BI and many GeoJSON consumers handle simple polygons better.
    for geom in geoms:
        if geom.get("type") == "Polygon":
            return geom
    return {"type": "GeometryCollection", "geometries": geoms}


def extract_essential_fields(name: Optional[str], desc: Optional[str], timezone_name: str) -> Dict[str, Any]:
    """Extract Essential Energy fields from the KML placemark description.

    This mirrors the useful parts of the user's previous Power Query logic:
      - Incident from <h2>...</h2>
      - Time Off
      - Est. Time On
      - No. of Customers affected
      - Reason
      - Last Updated

    It also keeps the older line-based fallback parser because the KML structure can vary.
    """
    decoded_html = decode_essential_html(desc)
    desc_text = strip_html_tags(decoded_html)

    incident = text_between(decoded_html, "<h2>", "</h2>")
    if incident:
        incident = strip_html_tags(incident)

    time_off_raw = essential_label_value(decoded_html, "Time Off:")
    est_time_on_raw = essential_label_value(decoded_html, "Est. Time On:")
    customers_raw = essential_label_value(decoded_html, "No. of Customers affected:")
    reason_raw = essential_label_value(decoded_html, "Reason:")
    last_updated_raw = essential_label_value(decoded_html, "Last Updated:")

    fields: Dict[str, Any] = {
        "title": as_str(incident) or as_str(name),
        "incident": as_str(incident),
        "status": None,
        "reason": as_str(reason_raw),
        "suburb": None,
        "street_name": None,
        "time_off_raw": as_str(time_off_raw),
        "est_time_on_raw": as_str(est_time_on_raw),
        "last_updated_raw": as_str(last_updated_raw),
        "start_utc": parse_au_local_datetime_to_utc_iso(time_off_raw, timezone_name=timezone_name),
        "etr_utc": parse_au_local_datetime_to_utc_iso(est_time_on_raw, timezone_name=timezone_name),
        "last_updated_utc": parse_au_local_datetime_to_utc_iso(last_updated_raw, timezone_name=timezone_name),
        "affected_customers": as_int(customers_raw),
        "description_html_decoded": decoded_html,
        "description_text": desc_text,
    }

    lines = [line.strip() for line in desc_text.splitlines() if line.strip()]
    key_value_pairs: Dict[str, str] = {}

    for line in lines:
        m = re.match(r"^([^:]{2,80})\s*:\s*(.+)$", line)
        if not m:
            continue
        key = re.sub(r"\s+", " ", m.group(1).strip().lower())
        value = m.group(2).strip()
        key_value_pairs[key] = value

    def first_kv(keys: Iterable[str]) -> Optional[str]:
        for key in keys:
            for existing_key, value in key_value_pairs.items():
                if existing_key == key or existing_key.replace(" ", "_") == key.replace(" ", "_"):
                    return value
        return None

    fields["status"] = fields["status"] or first_kv(["status"])
    fields["reason"] = fields["reason"] or first_kv(["cause", "reason", "outage cause"])
    fields["suburb"] = first_kv(["suburb", "suburbs", "locality", "town"])
    fields["street_name"] = first_kv(["street", "streets", "location", "affected area"])

    start_raw = fields["time_off_raw"] or first_kv(["time off", "start", "started", "start time", "outage start", "interruption start"])
    etr_raw = fields["est_time_on_raw"] or first_kv([
        "est. time on", "est time on", "estimated restoration", "estimated restore", "est restore",
        "restoration time", "estimated time of restoration", "etr", "finish", "end", "end time",
    ])
    fields["start_utc"] = fields["start_utc"] or parse_au_local_datetime_to_utc_iso(start_raw, timezone_name=timezone_name)
    fields["etr_utc"] = fields["etr_utc"] or parse_au_local_datetime_to_utc_iso(etr_raw, timezone_name=timezone_name)

    customer_raw = customers_raw or first_kv([
        "no. of customers affected", "no of customers affected", "customers affected",
        "affected customers", "customers", "customer count", "number of customers affected",
        "number customers affected",
    ])
    fields["affected_customers"] = fields["affected_customers"] if fields["affected_customers"] is not None else as_int(customer_raw)

    if fields["affected_customers"] is None and desc_text:
        m = re.search(
            r"(?:no\.?\s+of\s+customers\s+affected|customers?\s+affected|affected\s+customers?|customers?)\D{0,30}([0-9][0-9,]*)",
            desc_text,
            flags=re.I,
        )
        if m:
            fields["affected_customers"] = as_int(m.group(1))

    return fields


def parse_essential_kml(kml_text: str, source_url: str, timezone_name: str) -> List[OutageRecord]:
    root = ET.fromstring(kml_text)
    ns_uri = "http://www.opengis.net/kml/2.2"
    if root.tag.startswith("{") and "}" in root.tag:
        ns_uri = root.tag.split("}")[0].strip("{")
    ns = {"kml": ns_uri}

    records: List[OutageRecord] = []

    for idx, pm in enumerate(root.findall(".//kml:Placemark", ns), start=1):
        name = find_kml_text(pm, "kml:name", ns)
        desc = find_kml_text(pm, "kml:description", ns)
        geom = kml_placemark_geometry(pm, ns)
        centroid_lon, centroid_lat = geometry_centroid(geom)
        fields = extract_essential_fields(name, desc, timezone_name=timezone_name)
        outage_type = infer_outage_type(fields, source_hint=f"{name or ''}\n{fields.get('description_text') or ''}") or "unplanned"

        # Essential's KML has not consistently exposed a durable outage ID. Use a stable hash from
        # title/suburb/rounded centroid instead of the full description, because restoration/status text may change.
        key_seed = {
            "title": fields.get("title"),
            "suburb": fields.get("suburb"),
            "centroid_lon": round(float(centroid_lon), 5) if centroid_lon is not None else None,
            "centroid_lat": round(float(centroid_lat), 5) if centroid_lat is not None else None,
            "placemark_index": idx if centroid_lon is None or centroid_lat is None else None,
        }
        outage_key = make_outage_key(
            ESSENTIAL_PROVIDER_CODE,
            outage_id=None,
            outage_type=outage_type,
            centroid_lon=centroid_lon,
            centroid_lat=centroid_lat,
            start_utc=fields.get("start_utc"),
            suburb=fields.get("suburb"),
            raw=key_seed,
        )

        raw = {
            "raw_name": name,
            "raw_description_text": fields.get("description_text"),
            "raw_description_html_decoded": fields.get("description_html_decoded"),
            "essential_in_qld_fallback_polygon": point_in_qld_fallback_polygon(centroid_lon, centroid_lat),
            "parsed_fields": {
                k: v for k, v in fields.items()
                if k not in ("description_text", "description_html_decoded")
            },
        }

        records.append(
            OutageRecord(
                provider_code=ESSENTIAL_PROVIDER_CODE,
                provider_name=ESSENTIAL_PROVIDER_NAME,
                outage_key=outage_key,
                outage_id=None,
                outage_type=outage_type,
                status=as_str(fields.get("status")),
                reason=as_str(fields.get("reason")),
                affected_customers=fields.get("affected_customers") if isinstance(fields.get("affected_customers"), int) else None,
                suburb=as_str(fields.get("suburb")),
                street_name=as_str(fields.get("street_name")),
                start_utc=as_str(fields.get("start_utc")),
                etr_utc=as_str(fields.get("etr_utc")),
                centroid_lon=centroid_lon,
                centroid_lat=centroid_lat,
                source_url=source_url,
                geometry=geom,
                raw=raw,
            )
        )

    return dedupe_current_records(records)


# --------------------------------------------------------------------------------------
# Provider fetchers
# --------------------------------------------------------------------------------------

def fetch_energex(timeout: int, debug: bool, user_agent: str) -> List[OutageRecord]:
    records: List[OutageRecord] = []

    for url in ENERGEX_URLS:
        payload = http_get_json(url, timeout=timeout, user_agent=user_agent)

        feats_in: List[Any] = []
        if isinstance(payload, dict):
            feats_in = payload.get("features") or []
        if not isinstance(feats_in, list):
            feats_in = []

        for f in feats_in:
            if not isinstance(f, dict):
                continue

            props = f.get("properties") or {}
            if not isinstance(props, dict):
                props = {}

            pt, best_geom = extract_point_and_best_geom(f.get("geometry"))
            centroid_lon, centroid_lat = geometry_centroid(best_geom)

            # Preserve point if polygon centroid could not be calculated.
            if (centroid_lon is None or centroid_lat is None) and pt:
                centroid_lon, centroid_lat = pt

            outage_type = infer_outage_type(props, source_hint=url)
            outage_id = as_str(first_present(props, [
                "EVENT_ID", "INCIDENT_ID", "incidentId", "ID", "id", "OUTAGE_ID", "outageId"
            ]))
            start_utc = parse_epoch_any_to_utc_iso(first_present(props, [
                "START_TIME", "startTime", "START", "startDateTime", "STARTDATE", "outageStart"
            ]))
            etr_utc = parse_epoch_any_to_utc_iso(first_present(props, [
                "ETR", "etr", "EST_FIX_TIME", "ESTIMATED_RESTORE", "END_TIME", "endTime", "estimatedRestoration"
            ]))
            suburb = as_str(first_present(props, ["SUBURB", "SUBURBS", "suburb", "suburbs"]))
            affected = extract_customers_affected(props)

            outage_key = make_outage_key(
                "au.qld.energex",
                outage_id,
                outage_type,
                centroid_lon,
                centroid_lat,
                start_utc,
                suburb,
                props,
            )

            records.append(
                OutageRecord(
                    provider_code="au.qld.energex",
                    provider_name="Energex",
                    outage_key=outage_key,
                    outage_id=outage_id,
                    outage_type=outage_type,
                    status=as_str(first_present(props, ["STATUS", "status"])),
                    reason=as_str(first_present(props, ["CAUSE", "cause", "REASON", "reason"])),
                    affected_customers=affected,
                    suburb=suburb,
                    street_name=as_str(first_present(props, ["STREET", "STREETS", "street", "streets", "streetName"])),
                    start_utc=start_utc,
                    etr_utc=etr_utc,
                    centroid_lon=centroid_lon,
                    centroid_lat=centroid_lat,
                    source_url=url,
                    geometry=best_geom,
                    raw=props,
                )
            )

        if debug:
            print(f"[DEBUG] Energex fetched {len(feats_in)} features from {url}", file=sys.stderr)

    return dedupe_current_records(records)


def fetch_ergon(timeout: int, chunk_size: int, where: str, debug: bool, user_agent: str) -> List[OutageRecord]:
    oids, oid_field = arcgis_get_object_ids(ERGON_LAYER_URL, where=where, timeout=timeout, user_agent=user_agent)
    rows = arcgis_fetch_features_by_object_ids(
        ERGON_LAYER_URL,
        oids,
        where=where,
        timeout=timeout,
        chunk_size=max(1, chunk_size),
        user_agent=user_agent,
    )

    records: List[OutageRecord] = []
    sample_attrs: Optional[Dict[str, Any]] = None

    for r in rows:
        attrs = r.get("attributes") or {}
        if not isinstance(attrs, dict):
            continue
        if sample_attrs is None and attrs:
            sample_attrs = attrs

        geom = esri_geometry_to_geojson(r.get("geometry"))
        if geom is None:
            continue

        centroid_lon, centroid_lat = geometry_centroid(geom)
        outage_type = infer_outage_type(attrs)
        outage_id = (
            as_str(first_present(attrs, [
                "INCIDENT_ID", "incidentId", "IncidentId", "incident_id",
                "OUTAGE_ID", "outageId", "id", "ID",
            ]))
            or as_str(first_present(attrs, [oid_field, "OBJECTID"]))
        )
        start_utc = parse_epoch_any_to_utc_iso(first_present(attrs, [
            "START_TIME", "startTime", "startDateTime", "STARTDATE", "outageStart"
        ]))
        etr_utc = parse_epoch_any_to_utc_iso(first_present(attrs, [
            "ETR", "etr", "END_TIME", "endTime", "endDateTime", "estimatedRestoration"
        ]))
        suburb = as_str(first_present(attrs, ["SUBURB", "suburb"]))

        outage_key = make_outage_key(
            "au.qld.ergon",
            outage_id,
            outage_type,
            centroid_lon,
            centroid_lat,
            start_utc,
            suburb,
            attrs,
        )

        records.append(
            OutageRecord(
                provider_code="au.qld.ergon",
                provider_name="Ergon Energy",
                outage_key=outage_key,
                outage_id=outage_id,
                outage_type=outage_type,
                status=as_str(first_present(attrs, ["STATUS", "status"])),
                reason=as_str(first_present(attrs, ["CAUSE", "cause", "REASON", "reason"])),
                affected_customers=extract_customers_affected(attrs),
                suburb=suburb,
                street_name=as_str(first_present(attrs, ["streetName", "STREET", "street"])),
                start_utc=start_utc,
                etr_utc=etr_utc,
                centroid_lon=centroid_lon,
                centroid_lat=centroid_lat,
                source_url=ERGON_LAYER_URL,
                geometry=geom,
                raw=attrs,
            )
        )

    if debug:
        print(f"[DEBUG] Ergon objectIds={len(oids)} rows={len(rows)} output_records={len(records)}", file=sys.stderr)
        if sample_attrs:
            print(f"[DEBUG] Ergon sample keys={sorted(sample_attrs.keys())[:100]}", file=sys.stderr)

    return dedupe_current_records(records)


def fetch_essential_energy(url: str, timeout: int, debug: bool, user_agent: str, timezone_name: str) -> List[OutageRecord]:
    kml_text = http_get_text(url, timeout=timeout, user_agent=user_agent)
    records = parse_essential_kml(kml_text, source_url=url, timezone_name=timezone_name)
    if debug:
        print(f"[DEBUG] Essential Energy KML output_records={len(records)} from {url}", file=sys.stderr)
    return records


def dedupe_current_records(records: List[OutageRecord]) -> List[OutageRecord]:
    """
    One row per outage key in the current run. If a duplicate appears, prefer the
    one with a known affected customer count and geometry.
    """
    best: Dict[str, OutageRecord] = {}
    for r in records:
        existing = best.get(r.outage_key)
        if existing is None:
            best[r.outage_key] = r
            continue

        score_new = (1 if r.affected_customers is not None else 0) + (1 if r.geometry else 0)
        score_old = (1 if existing.affected_customers is not None else 0) + (1 if existing.geometry else 0)
        if score_new > score_old:
            best[r.outage_key] = r

    return list(best.values())


def is_unplanned_outage_type(value: Optional[str]) -> bool:
    """Return True only for records explicitly classified as unplanned/unscheduled."""
    if value is None:
        return False
    v = str(value).strip().lower()
    return v in {"unplanned", "unscheduled", "un-scheduled"} or "unplanned" in v or "unscheduled" in v


def is_unplanned_record(record: OutageRecord) -> bool:
    return is_unplanned_outage_type(record.outage_type)


def is_unplanned_snapshot_row(row: Dict[str, str]) -> bool:
    return is_unplanned_outage_type(row.get("outage_type"))


# --------------------------------------------------------------------------------------
# LGA assignment
# --------------------------------------------------------------------------------------

def load_lga_cache(cache_path: Path) -> Dict[str, Dict[str, Optional[str]]]:
    if not cache_path.exists():
        return {}
    try:
        obj = json.loads(cache_path.read_text(encoding="utf-8"))
        if isinstance(obj, dict):
            out: Dict[str, Dict[str, Optional[str]]] = {}
            for k, v in obj.items():
                if isinstance(v, dict):
                    out[k] = {
                        "lga_name": as_str(v.get("lga_name")),
                        "lga_code": as_str(v.get("lga_code")),
                    }
            return out
    except Exception:
        pass
    return {}


def save_lga_cache(cache_path: Path, cache: Dict[str, Dict[str, Optional[str]]]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def lga_cache_key(lon: Optional[float], lat: Optional[float], precision: int = 5) -> Optional[str]:
    if lon is None or lat is None:
        return None
    return f"{round(float(lon), precision)},{round(float(lat), precision)}"


def lookup_lga_for_point(
    lon: Optional[float],
    lat: Optional[float],
    cache: Dict[str, Dict[str, Optional[str]]],
    timeout: int,
    user_agent: str,
    cache_precision: int,
) -> Tuple[Optional[str], Optional[str]]:
    key = lga_cache_key(lon, lat, precision=cache_precision)
    if key is None or lon is None or lat is None:
        return None, None

    cached = cache.get(key)
    if cached is not None:
        return cached.get("lga_name"), cached.get("lga_code")

    params = {
        "f": "json",
        "where": "1=1",
        "returnGeometry": "false",
        "outFields": "lga,lga_code,adminareaname,abbrev_name,admintypename",
        "geometry": json.dumps({"x": float(lon), "y": float(lat), "spatialReference": {"wkid": 4326}}),
        "geometryType": "esriGeometryPoint",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outSR": "4326",
        "resultRecordCount": "1",
    }

    try:
        j = arcgis_query(QLD_LGA_LAYER_URL, params, timeout=timeout, user_agent=user_agent)
        feats = j.get("features") or []
        if isinstance(feats, list) and feats:
            attrs = feats[0].get("attributes") or {}
            if isinstance(attrs, dict):
                lga_name = (
                    as_str(attrs.get("lga"))
                    or as_str(attrs.get("adminareaname"))
                    or as_str(attrs.get("abbrev_name"))
                    or as_str(attrs.get("admintypename"))
                )
                lga_code = as_str(attrs.get("lga_code"))
                cache[key] = {"lga_name": lga_name, "lga_code": lga_code}
                return lga_name, lga_code
    except Exception:
        # Cache misses should not fail the whole outage pipeline.
        pass

    cache[key] = {"lga_name": None, "lga_code": None}
    return None, None


def assign_lgas(
    records: List[OutageRecord],
    cache_path: Path,
    timeout: int,
    user_agent: str,
    cache_precision: int,
    debug: bool,
) -> None:
    cache = load_lga_cache(cache_path)
    before = len(cache)
    matched = 0
    unmatched = 0

    for r in records:
        lga_name, lga_code = lookup_lga_for_point(
            r.centroid_lon,
            r.centroid_lat,
            cache=cache,
            timeout=timeout,
            user_agent=user_agent,
            cache_precision=cache_precision,
        )
        r.lga_name = lga_name or "Unmatched"
        r.lga_code = lga_code
        if lga_name:
            matched += 1
        else:
            unmatched += 1

    save_lga_cache(cache_path, cache)

    if debug:
        print(
            f"[DEBUG] LGA assignment matched={matched} unmatched={unmatched} "
            f"cache_before={before} cache_after={len(cache)}",
            file=sys.stderr,
        )


# --------------------------------------------------------------------------------------
# CSV / output builders
# --------------------------------------------------------------------------------------

def snapshot_row(record: OutageRecord, snapshot_dt: datetime) -> Dict[str, str]:
    affected_known = record.affected_customers is not None
    geom_type = record.geometry.get("type") if isinstance(record.geometry, dict) else None

    return {
        "snapshot_utc": to_utc_iso(snapshot_dt),
        "snapshot_aest": to_aest_iso(snapshot_dt),
        "provider_code": record.provider_code,
        "provider_name": record.provider_name,
        "outage_key": record.outage_key,
        "outage_id": record.outage_id or "",
        "outage_type": record.outage_type or "unknown",
        "status": record.status or "",
        "reason": record.reason or "",
        "affected_customers": "" if record.affected_customers is None else str(record.affected_customers),
        "affected_customers_known": "true" if affected_known else "false",
        "suburb": record.suburb or "",
        "street_name": record.street_name or "",
        "start_utc": record.start_utc or "",
        "etr_utc": record.etr_utc or "",
        "centroid_lon": "" if record.centroid_lon is None else f"{record.centroid_lon:.7f}",
        "centroid_lat": "" if record.centroid_lat is None else f"{record.centroid_lat:.7f}",
        "lga_name": record.lga_name or "Unmatched",
        "lga_code": record.lga_code or "",
        "source_url": record.source_url,
        "geometry_type": geom_type or "",
        "geometry_json": json_dumps_compact(record.geometry) if record.geometry else "",
        "raw_json": json_dumps_compact(record.raw),
    }


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return [{k: (v if v is not None else "") for k, v in row.items()} for row in reader]


def write_csv_rows(path: Path, rows: List[Dict[str, Any]], columns: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c, "") for c in columns})
    tmp_path.replace(path)


def dedupe_snapshot_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    # If a workflow is manually re-run with the same snapshot timestamp, keep the latest row.
    seen: Dict[Tuple[str, str], Dict[str, str]] = {}
    for row in rows:
        key = (row.get("snapshot_utc", ""), row.get("outage_key", ""))
        seen[key] = row
    out = list(seen.values())
    out.sort(key=lambda r: (r.get("snapshot_utc", ""), r.get("provider_code", ""), r.get("lga_name", ""), r.get("outage_key", "")))
    return out


def trim_to_window(rows: List[Dict[str, str]], now_dt: datetime, hours: int) -> List[Dict[str, str]]:
    cutoff = now_dt - timedelta(hours=hours)
    out: List[Dict[str, str]] = []
    for row in rows:
        dt = parse_utc_iso(row.get("snapshot_utc"))
        if dt is None:
            continue
        if dt >= cutoff:
            out.append(row)
    return out


def int_from_row(row: Dict[str, str], col: str, default: int = 0) -> int:
    value = as_int(row.get(col))
    return default if value is None else value


def build_lga_totals(snapshot_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """
    Build provider-specific rows plus an 'all' provider row for each LGA/snapshot.
    """
    group: Dict[Tuple[str, str, str, str, str, str], Dict[str, Any]] = {}

    def add(row: Dict[str, str], provider_code: str, provider_name: str) -> None:
        snapshot_utc = row.get("snapshot_utc", "")
        snapshot_aest = row.get("snapshot_aest", "")
        lga_name = row.get("lga_name") or "Unmatched"
        lga_code = row.get("lga_code") or ""
        key = (snapshot_utc, snapshot_aest, lga_name, lga_code, provider_code, provider_name)

        if key not in group:
            group[key] = {
                "snapshot_utc": snapshot_utc,
                "snapshot_aest": snapshot_aest,
                "lga_name": lga_name,
                "lga_code": lga_code,
                "provider_code": provider_code,
                "provider_name": provider_name,
                "active_outage_count": 0,
                "known_customer_count_outage_count": 0,
                "unknown_customer_count_outage_count": 0,
                "total_affected_customers": 0,
                "planned_customers": 0,
                "unplanned_customers": 0,
                "unknown_type_customers": 0,
            }

        g = group[key]
        customers_known = str(row.get("affected_customers_known", "")).lower() == "true"
        customers = int_from_row(row, "affected_customers", default=0)
        outage_type = (row.get("outage_type") or "unknown").strip().lower()

        g["active_outage_count"] += 1
        if customers_known:
            g["known_customer_count_outage_count"] += 1
        else:
            g["unknown_customer_count_outage_count"] += 1

        g["total_affected_customers"] += customers
        if outage_type == "planned":
            g["planned_customers"] += customers
        elif outage_type == "unplanned":
            g["unplanned_customers"] += customers
        else:
            g["unknown_type_customers"] += customers

    for row in snapshot_rows:
        add(row, row.get("provider_code", "") or "unknown", row.get("provider_name", "") or "Unknown")
        add(row, "all", "All providers")

    rows = list(group.values())
    rows.sort(key=lambda r: (r["snapshot_utc"], r["lga_name"], r["provider_code"]))
    return rows


def latest_rows_by_snapshot(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return []
    latest = max(str(r.get("snapshot_utc", "")) for r in rows)
    return [r for r in rows if str(r.get("snapshot_utc", "")) == latest]


def build_lga_cumulative(
    lga_rows: List[Dict[str, Any]],
    now_dt: datetime,
    window_hours: int,
    poll_interval_minutes: int,
    max_interval_minutes: int,
) -> List[Dict[str, Any]]:
    """
    Estimate customer-minutes/customer-hours from the time-series table.

    For each LGA/provider group, each snapshot contributes:
      affected_customers * minutes_until_next_snapshot

    To avoid over-counting long gaps from missed scheduled runs, the contribution
    interval is capped by max_interval_minutes. The final latest snapshot uses
    poll_interval_minutes.
    """
    window_start = now_dt - timedelta(hours=window_hours)
    window_end = now_dt

    groups: Dict[Tuple[str, str, str, str], List[Dict[str, Any]]] = {}
    for row in lga_rows:
        key = (
            str(row.get("lga_name", "") or "Unmatched"),
            str(row.get("lga_code", "") or ""),
            str(row.get("provider_code", "") or "unknown"),
            str(row.get("provider_name", "") or "Unknown"),
        )
        groups.setdefault(key, []).append(row)

    out: List[Dict[str, Any]] = []

    for (lga_name, lga_code, provider_code, provider_name), rows in groups.items():
        parsed: List[Tuple[datetime, Dict[str, Any]]] = []
        for row in rows:
            dt = parse_utc_iso(row.get("snapshot_utc"))
            if dt is None:
                continue
            parsed.append((dt, row))
        parsed.sort(key=lambda x: x[0])
        if not parsed:
            continue

        customer_snapshot_sum = 0
        customer_minutes = 0.0
        max_customers = 0

        for idx, (dt, row) in enumerate(parsed):
            customers = int_from_row({k: str(v) for k, v in row.items()}, "total_affected_customers", default=0)
            customer_snapshot_sum += customers
            max_customers = max(max_customers, customers)

            if idx + 1 < len(parsed):
                next_dt = parsed[idx + 1][0]
                minutes = max(0.0, (next_dt - dt).total_seconds() / 60.0)
                minutes = min(minutes, float(max_interval_minutes))
            else:
                minutes = float(poll_interval_minutes)

            customer_minutes += customers * minutes

        latest_dt, latest_row = parsed[-1]
        latest_customers = int_from_row({k: str(v) for k, v in latest_row.items()}, "total_affected_customers", default=0)

        out.append({
            "window_start_utc": to_utc_iso(window_start),
            "window_end_utc": to_utc_iso(window_end),
            "lga_name": lga_name,
            "lga_code": lga_code,
            "provider_code": provider_code,
            "provider_name": provider_name,
            "snapshot_count": len(parsed),
            "latest_snapshot_utc": to_utc_iso(latest_dt),
            "latest_affected_customers": latest_customers,
            "max_affected_customers": max_customers,
            "customer_snapshot_sum_48h": customer_snapshot_sum,
            "estimated_customer_minutes_48h": round(customer_minutes, 2),
            "estimated_customer_hours_48h": round(customer_minutes / 60.0, 4),
            "estimated_customer_days_48h": round(customer_minutes / 1440.0, 4),
        })

    out.sort(key=lambda r: (r["lga_name"], r["provider_code"]))
    return out


def current_geojson(records: List[OutageRecord], snapshot_dt: datetime) -> Dict[str, Any]:
    features: List[Dict[str, Any]] = []
    for r in records:
        props = {
            "snapshot_utc": to_utc_iso(snapshot_dt),
            "snapshot_aest": to_aest_iso(snapshot_dt),
            "provider_code": r.provider_code,
            "provider_name": r.provider_name,
            "outage_key": r.outage_key,
            "outage_id": r.outage_id,
            "outage_type": r.outage_type or "unknown",
            "status": r.status,
            "reason": r.reason,
            "affected_customers": r.affected_customers,
            "affected_customers_known": r.affected_customers is not None,
            "suburb": r.suburb,
            "street_name": r.street_name,
            "start_utc": r.start_utc,
            "etr_utc": r.etr_utc,
            "centroid_lon": r.centroid_lon,
            "centroid_lat": r.centroid_lat,
            "lga_name": r.lga_name or "Unmatched",
            "lga_code": r.lga_code,
            "source_url": r.source_url,
        }
        features.append({"type": "Feature", "geometry": r.geometry, "properties": props})
    return {"type": "FeatureCollection", "features": features}


# --------------------------------------------------------------------------------------
# Main pipeline
# --------------------------------------------------------------------------------------

def run_pipeline(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    snapshot_path = output_dir / "outage_snapshots_48h.csv"
    lga_totals_path = output_dir / "lga_totals_48h.csv"
    lga_totals_latest_path = output_dir / "lga_totals_latest.csv"
    lga_cumulative_path = output_dir / "lga_cumulative_48h.csv"
    current_geojson_path = output_dir / "current_outages.geojson"
    manifest_path = output_dir / "run_manifest.json"
    lga_cache_path = output_dir / "lga_lookup_cache.json"

    snapshot_dt = utc_now()
    if args.snapshot_utc:
        parsed = parse_utc_iso(args.snapshot_utc)
        if parsed is None:
            raise ValueError(f"Invalid --snapshot-utc value: {args.snapshot_utc}")
        snapshot_dt = parsed

    user_agent = args.user_agent

    all_current: List[OutageRecord] = []
    provider_fetch_errors: Dict[str, str] = {}

    def fetch_provider(provider_name: str, func: Any) -> List[OutageRecord]:
        try:
            records = func()
            all_current.extend(records)
            return records
        except Exception as e:
            provider_fetch_errors[provider_name] = f"{type(e).__name__}: {e}"
            if args.debug:
                print(f"[WARN] {provider_name} fetch failed: {type(e).__name__}: {e}", file=sys.stderr)
            if args.strict_provider_failures:
                raise
            return []

    if not args.skip_energex:
        energex = fetch_provider(
            "Energex",
            lambda: fetch_energex(timeout=args.timeout, debug=args.debug, user_agent=user_agent),
        )
    else:
        energex = []

    if not args.skip_ergon:
        ergon = fetch_provider(
            "Ergon",
            lambda: fetch_ergon(
                timeout=args.timeout,
                chunk_size=args.ergon_chunk_size,
                where=args.ergon_where,
                debug=args.debug,
                user_agent=user_agent,
            ),
        )
    else:
        ergon = []

    if not args.skip_essential:
        essential = fetch_provider(
            "Essential Energy",
            lambda: fetch_essential_energy(
                url=args.essential_url,
                timeout=args.timeout,
                debug=args.debug,
                user_agent=user_agent,
                timezone_name=args.essential_timezone,
            ),
        )
    else:
        essential = []

    all_current = dedupe_current_records(all_current)

    current_records_before_outage_type_filter = len(all_current)
    if not args.include_planned:
        all_current = [r for r in all_current if is_unplanned_record(r)]
    current_records_after_outage_type_filter = len(all_current)
    current_records_dropped_non_unplanned = (
        current_records_before_outage_type_filter - current_records_after_outage_type_filter
    )

    if args.debug and current_records_dropped_non_unplanned:
        print(
            f"[DEBUG] Unplanned-only filter kept={current_records_after_outage_type_filter} "
            f"dropped_planned_or_unknown={current_records_dropped_non_unplanned}",
            file=sys.stderr,
        )

    essential_before_polygon_filter = len([r for r in all_current if r.provider_code == ESSENTIAL_PROVIDER_CODE])
    if not args.disable_essential_qld_polygon_prefilter:
        all_current = [
            r for r in all_current
            if r.provider_code != ESSENTIAL_PROVIDER_CODE
            or point_in_qld_fallback_polygon(r.centroid_lon, r.centroid_lat)
        ]
    essential_after_polygon_filter = len([r for r in all_current if r.provider_code == ESSENTIAL_PROVIDER_CODE])
    essential_dropped_by_qld_polygon = essential_before_polygon_filter - essential_after_polygon_filter

    if args.debug and essential_before_polygon_filter:
        print(
            f"[DEBUG] Essential QLD fallback polygon prefilter kept={essential_after_polygon_filter} "
            f"dropped={essential_dropped_by_qld_polygon}",
            file=sys.stderr,
        )

    assign_lgas(
        all_current,
        cache_path=lga_cache_path,
        timeout=args.timeout,
        user_agent=user_agent,
        cache_precision=args.lga_cache_precision,
        debug=args.debug,
    )

    essential_before_qld_filter = len([r for r in all_current if r.provider_code == ESSENTIAL_PROVIDER_CODE])
    if not args.include_unmatched_essential:
        all_current = [
            r for r in all_current
            if r.provider_code != ESSENTIAL_PROVIDER_CODE or (r.lga_name and r.lga_name != "Unmatched")
        ]
    essential_after_qld_filter = len([r for r in all_current if r.provider_code == ESSENTIAL_PROVIDER_CODE])
    essential_dropped_non_qld_or_unmatched = essential_before_qld_filter - essential_after_qld_filter

    if args.debug and essential_before_qld_filter:
        print(
            f"[DEBUG] Essential QLD filter kept={essential_after_qld_filter} "
            f"dropped_non_qld_or_unmatched={essential_dropped_non_qld_or_unmatched}",
            file=sys.stderr,
        )

    new_snapshot_rows = [snapshot_row(r, snapshot_dt=snapshot_dt) for r in all_current]

    existing_rows = read_csv_rows(snapshot_path)
    if not args.include_planned:
        existing_rows = [row for row in existing_rows if is_unplanned_snapshot_row(row)]
    combined_rows = existing_rows + new_snapshot_rows
    combined_rows = trim_to_window(combined_rows, now_dt=snapshot_dt, hours=args.window_hours)
    combined_rows = dedupe_snapshot_rows(combined_rows)

    lga_totals_rows = build_lga_totals(combined_rows)
    lga_totals_latest_rows = latest_rows_by_snapshot(lga_totals_rows)
    lga_cumulative_rows = build_lga_cumulative(
        lga_totals_rows,
        now_dt=snapshot_dt,
        window_hours=args.window_hours,
        poll_interval_minutes=args.poll_interval_minutes,
        max_interval_minutes=args.max_interval_minutes,
    )

    write_csv_rows(snapshot_path, combined_rows, SNAPSHOT_COLUMNS)
    write_csv_rows(lga_totals_path, lga_totals_rows, LGA_TOTAL_COLUMNS)
    write_csv_rows(lga_totals_latest_path, lga_totals_latest_rows, LGA_TOTAL_COLUMNS)
    write_csv_rows(lga_cumulative_path, lga_cumulative_rows, CUMULATIVE_COLUMNS)

    current_geojson_path.write_text(
        json.dumps(current_geojson(all_current, snapshot_dt), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    manifest = {
        "generated_utc": to_utc_iso(snapshot_dt),
        "generated_aest": to_aest_iso(snapshot_dt),
        "window_hours": args.window_hours,
        "poll_interval_minutes": args.poll_interval_minutes,
        "current_outage_records": len(all_current),
        "outage_type_filter": "all" if args.include_planned else "unplanned_only",
        "current_records_before_outage_type_filter": current_records_before_outage_type_filter,
        "current_records_after_outage_type_filter": current_records_after_outage_type_filter,
        "current_records_dropped_planned_or_unknown": current_records_dropped_non_unplanned,
        "provider_fetch_errors": provider_fetch_errors,
        "provider_fetch_error_count": len(provider_fetch_errors),
        "current_energex_records": len([r for r in all_current if r.provider_code == "au.qld.energex"]),
        "current_ergon_records": len([r for r in all_current if r.provider_code == "au.qld.ergon"]),
        "current_essential_records_fetched": len(essential),
        "current_essential_records_after_qld_polygon_prefilter": essential_after_polygon_filter,
        "current_essential_records_dropped_by_qld_polygon_prefilter": essential_dropped_by_qld_polygon,
        "current_essential_records_included_qld": len([r for r in all_current if r.provider_code == ESSENTIAL_PROVIDER_CODE]),
        "current_essential_records_dropped_non_qld_or_unmatched": essential_dropped_non_qld_or_unmatched,
        "snapshot_rows_48h": len(combined_rows),
        "lga_totals_rows_48h": len(lga_totals_rows),
        "lga_totals_latest_rows": len(lga_totals_latest_rows),
        "lga_cumulative_rows": len(lga_cumulative_rows),
        "outputs": {
            "current_outages_geojson": str(current_geojson_path),
            "outage_snapshots_48h_csv": str(snapshot_path),
            "lga_totals_48h_csv": str(lga_totals_path),
            "lga_totals_latest_csv": str(lga_totals_latest_path),
            "lga_cumulative_48h_csv": str(lga_cumulative_path),
            "lga_lookup_cache_json": str(lga_cache_path),
        },
        "sources": {
            "energex_urls": ENERGEX_URLS,
            "ergon_layer_url": ERGON_LAYER_URL,
            "essential_energy_kml_url": args.essential_url,
            "qld_lga_layer_url": QLD_LGA_LAYER_URL,
        },
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.debug:
        print(json.dumps(manifest, indent=2), file=sys.stderr)

    print(
        f"[OK] current={len(all_current)} snapshots_48h={len(combined_rows)} "
        f"lga_totals_48h={len(lga_totals_rows)} output_dir={output_dir}"
    )
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Fetch QLD power outages, including QLD Essential Energy records, and build rolling 48-hour LGA datasets for Power BI.")
    ap.add_argument("--output-dir", default="data", help="Output folder. Default: data")
    ap.add_argument("--window-hours", type=int, default=48, help="Rolling history window. Default: 48")
    ap.add_argument("--poll-interval-minutes", type=int, default=15, help="Expected schedule interval. Default: 15")
    ap.add_argument("--max-interval-minutes", type=int, default=30, help="Cap used for customer-hour calculation if scheduled runs are missed. Default: 30")
    ap.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds. Default: 60")
    ap.add_argument("--ergon-chunk-size", type=int, default=250, help="ArcGIS objectId chunk size. Default: 250")
    ap.add_argument("--ergon-where", default="1=1", help="ArcGIS where clause for Ergon. Default: 1=1")
    ap.add_argument("--essential-url", default=ESSENTIAL_ENERGY_KML_URL, help="Essential Energy current outage KML URL.")
    ap.add_argument(
        "--essential-timezone",
        default="Australia/Sydney",
        help="Timezone used to interpret Essential Energy KML Time Off / Est. Time On fields when no timezone is supplied. Default: Australia/Sydney",
    )
    ap.add_argument(
        "--disable-essential-qld-polygon-prefilter",
        action="store_true",
        help="Do not pre-filter Essential Energy records through the built-in Queensland fallback polygon before LGA lookup.",
    )
    ap.add_argument("--lga-cache-precision", type=int, default=5, help="Decimal places for lon/lat LGA lookup cache key. Default: 5")
    ap.add_argument("--snapshot-utc", default="", help="Optional fixed snapshot UTC timestamp, mainly for testing.")
    ap.add_argument("--skip-energex", action="store_true", help="Skip Energex feed.")
    ap.add_argument("--skip-ergon", action="store_true", help="Skip Ergon feed.")
    ap.add_argument("--skip-essential", action="store_true", help="Skip Essential Energy feed.")
    ap.add_argument(
        "--include-planned",
        action="store_true",
        help="Include planned/scheduled outages. Default is unplanned-only for Power BI reporting.",
    )
    ap.add_argument(
        "--strict-provider-failures",
        action="store_true",
        help="Fail the whole run if any provider fetch fails. Default is to continue with the providers that worked and record errors in run_manifest.json.",
    )
    ap.add_argument(
        "--include-unmatched-essential",
        action="store_true",
        help="Keep Essential Energy records that do not resolve to a Queensland LGA. Default is to drop them.",
    )
    ap.add_argument("--debug", action="store_true", help="Print debug output to stderr.")
    ap.add_argument(
        "--user-agent",
        default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        help="HTTP User-Agent header. Default is browser-like because some public outage endpoints reject script-style user agents.",
    )
    return ap


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    return run_pipeline(args)


if __name__ == "__main__":
    raise SystemExit(main())
