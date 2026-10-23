"""Pre-fetch NOAA GFS 0.25 deg for East Asia from the NOMADS grib filter, per cycle.

The app asks NOMADS for one GRIB2 per (cycle, lead) -- 100-160E, 5-55N, height,
temperature, wind and humidity on 9 levels, surface pressure and MSLP -- and NOMADS
asks for 10 s between requests, so a phone needs about 3 minutes for a cycle. This
script makes exactly the app's request (GfsProxy.filterUrl) here instead and uploads
each GRIB2 unchanged to the release "gfs-<cycle>" as "gfs-<cycle>-fLLL-v1.grib2" -- the
app's own cache file name -- with gfs-manifest.json on the release "index". A cycle is
"complete" once all leads +0-48 h are in; the app shows only complete cycles.

Data: NOAA/NCEP GFS (public domain), unchanged regional subsets from NOMADS.
"""
import argparse
import json
import shutil
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import kim_relay as relay
from kim_relay import LEADS, drop_old_cycles, ensure_release, is_complete, load_manifest, log, publish

FILTER_URL = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
LEVELS = [1000, 950, 925, 850, 700, 500, 300, 200, 100]
VARIABLES = ["HGT", "TMP", "UGRD", "VGRD", "RH", "PRES", "PRMSL"]
# GfsProxy.VARIANT: the app's cache files of this request are named "...-v1.grib2".
VARIANT = "v1"
MANIFEST = "gfs-manifest.json"
ATTRIBUTION = ("NOAA/NCEP GFS 0.25° (public domain). NOMADS grib filter에서 동아시아 영역(100–160°E, 5–55°N)을 "
               "받아 그대로 올린 GRIB2입니다. NOAA가 이 저장소나 앱을 보증하지 않습니다.")
SPACING_S = 10  # NOMADS: at least 10 s between filter requests
RETRY_PAUSE_S = 60  # NWS: wait a minute after a failure
MAX_ATTEMPTS = 3
AVAILABILITY_LAG_HOURS = 3
PROBE_CANDIDATES = 4

_last_request = [0.0]


class NotYet(Exception):
    """NOMADS 404: this lead is not produced yet."""


def filter_url(cycle, lead):
    """The same request as GfsProxy.filterUrl."""
    params = [("file", "gfs.t%sz.pgrb2.0p25.f%03d" % (cycle[8:], lead)),
              ("dir", "/gfs.%s/%s/atmos" % (cycle[:8], cycle[8:])),
              ("subregion", ""), ("leftlon", "100"), ("rightlon", "160"), ("toplat", "55"), ("bottomlat", "5")]
    params += [("lev_%d_mb" % level, "on") for level in LEVELS]
    params += [("lev_surface", "on"), ("lev_mean_sea_level", "on")]
    params += [("var_%s" % name, "on") for name in VARIABLES]
    return FILTER_URL + "?" + urlencode(params)


def grib_problem(payload):
    """None when the payload is a whole sequence of GRIB2 messages (GfsProxy.gribProblem)."""
    pos = messages = 0
    while pos < len(payload):
        if len(payload) - pos < 16 or payload[pos:pos + 4] != b"GRIB" or payload[pos + 7] != 2:
            return "not a GRIB2 sequence" if messages == 0 else "truncated GRIB2"
        length = int.from_bytes(payload[pos + 8:pos + 16], "big")
        if length < 16 or pos + length > len(payload) or payload[pos + length - 4:pos + length] != b"7777":
            return "truncated GRIB2"
        pos += length
        messages += 1
    return None if messages else "empty answer"


def download(cycle, lead):
    last = "no attempt"
    for attempt in range(MAX_ATTEMPTS):
        wait = _last_request[0] + (RETRY_PAUSE_S if attempt else SPACING_S) - time.time()
        if wait > 0:
            time.sleep(wait)
        _last_request[0] = time.time()
        try:
            with urlopen(Request(filter_url(cycle, lead), headers={"User-Agent": "kma-kim-relay"}), timeout=60) as response:
                payload = response.read()
        except HTTPError as error:
            if error.code == 404:
                raise NotYet() from None
            last = "HTTP %d" % error.code
            continue
        except Exception as error:  # noqa: BLE001 - network errors are retried
            last = "%s: %s" % (type(error).__name__, error)
            continue
        finally:
            _last_request[0] = time.time()
        problem = grib_problem(payload)
        if problem is None:
            return payload
        last = problem
    raise relay.Failed(last)


def candidate_cycles(now=None):
    newest = (now or datetime.now(timezone.utc)) - timedelta(hours=AVAILABILITY_LAG_HOURS)
    newest = newest.replace(minute=0, second=0, microsecond=0, hour=newest.hour - newest.hour % 6)
    return [(newest - timedelta(hours=6 * i)).strftime("%Y%m%d%H") for i in range(PROBE_CANDIDATES)]


def run(manifest, cycle, leads, out_dir, dry_run, started, time_budget):
    """Fetch the leads of `cycle` not published yet; False if NOMADS has no +0 h for it yet."""
    entry = next((c for c in manifest["cycles"] if c["cycle"] == cycle), None)
    if entry is None:
        entry = {"cycle": cycle, "tag": "gfs-" + cycle, "leads": {}}
    todo = [lead for lead in leads if str(lead) not in entry["leads"]]
    log("GFS cycle", cycle, "to fetch:", todo)
    for lead in todo:
        if time.time() - started > time_budget:
            log("time budget used up; the next run continues")
            break
        try:
            payload = download(cycle, lead)
        except NotYet:
            log("+%dh not on NOMADS yet" % lead)
            if lead == 0:
                return False
            break
        except relay.Failed as error:
            log("+%dh failed:" % lead, error)
            continue
        if entry not in manifest["cycles"]:
            manifest["cycles"].append(entry)
            if not dry_run:
                ensure_release(entry["tag"], "GFS " + cycle, ATTRIBUTION)
        file = Path(out_dir) / ("gfs-%s-f%03d-%s.grib2" % (cycle, lead, VARIANT))
        file.write_bytes(payload)
        entry["leads"][str(lead)] = {"file": file.name, "bytes": len(payload), "missing": []}
        entry["complete"] = is_complete(entry)
        drop_old_cycles(manifest, dry_run)
        publish(manifest, out_dir, [(entry["tag"], str(file))], dry_run, name=MANIFEST)
        log("+%dh published: %d bytes" % (lead, len(payload)))
    log("GFS cycle %s %s" % (cycle, "complete" if entry.get("complete") else "not complete yet"))
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cycle", help="YYYYMMDDHH; default: the newest GFS cycle on NOMADS")
    parser.add_argument("--leads", help="comma separated; default: 0-48 h every 3 h")
    parser.add_argument("--out", help="keep the files in this folder")
    parser.add_argument("--dry-run", action="store_true", help="do not touch GitHub releases")
    parser.add_argument("--time-budget", type=float, default=30 * 60, help="seconds")
    args = parser.parse_args()
    leads = [int(item) for item in args.leads.split(",")] if args.leads else LEADS
    out_dir = args.out or tempfile.mkdtemp(prefix="gfs-")
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    started = time.time()
    manifest = load_manifest(MANIFEST, ATTRIBUTION)
    manifest["attribution"] = ATTRIBUTION
    try:
        for cycle in [args.cycle] if args.cycle else candidate_cycles():
            entry = next((c for c in manifest["cycles"] if c["cycle"] == cycle), None)
            if entry and entry.get("complete") and not args.cycle:
                log("GFS cycle", cycle, "is complete; nothing newer on NOMADS yet")
                break
            # Newest first: a cycle NOMADS does not have yet gives way to the one before.
            if run(manifest, cycle, leads, out_dir, args.dry_run, started, args.time_budget):
                break
    finally:
        if not args.out:
            shutil.rmtree(out_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
