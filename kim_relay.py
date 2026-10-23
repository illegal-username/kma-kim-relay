"""Pre-fetch KMA KIM forecast fields for East Asia and publish them as compact bundles.

Each run looks for the newest KIM cycle, fetches the leads that are not published yet
(or were published with fields missing, of which only the missing ones are fetched again)
and uploads one bundle per lead to the GitHub release "kim-<cycle>", then rewrites
manifest.json on the release "index". Fields that fail are retried in a few rounds within
the run and again by the next run. A cycle is marked "complete" once every lead has every
field; the app shows only complete cycles, so it never has to ask KMA itself. Cycles from
the newest down to the newest complete one are kept (at most three).

A bundle ("KIMR") holds the 47 fields of one lead -- height, temperature, wind and
relative humidity on 9 pressure levels, MSLP and surface pressure -- on every third point of the app's 721x601
window (241x201, 0.25 deg), each field in the app's KQ16 format (FieldCodec.java).

출처: 기상청. The data is re-processed here (thinned and quantised), so it is not the
original KMA data and not an official forecast; the bundle header and the manifest say so.
"""
import argparse
import json
import os
import re
import shutil
import struct
import subprocess
import sys
import tempfile
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import numpy as np

URL = "https://apihub.kma.go.kr/api/typ06/cgi-bin/url/nph-kim_nc_xy_txt2_std"
# 100-160E, 5-55N of the g576 grid (1-based x_min,y_min,x_max,y_max): the app's window.
WINDOW = "1201,1141,1921,1741"
PROBE_WINDOW = "1201,1141,1201,1141"
WIDTH, HEIGHT = 721, 601
STRIDE = 3
LEVELS = [1000, 950, 925, 850, 700, 500, 300, 200, 100]
LEADS = list(range(0, 49, 3))
# (API name, data kind, level, required, KQ16 decimals: 0.1 gpm, 0.01 K or m/s, 1 Pa)
FIELDS = (
    [("hgt", "P", level, True, 1) for level in LEVELS]
    + [("psl", "U", 0, True, 0), ("ps", "U", 0, False, 0)]
    + [(name, "P", level, False, 2) for name in ("T", "u", "v") for level in LEVELS]
    # Relative humidity (%, 0.1 steps): the app's cloud (humid air) layer.
    + [("rh", "P", level, False, 1) for level in LEVELS]
)
FORMAT = 1
ATTRIBUTION = (
    "출처: 기상청 (KIM 전구모델 KIMG/NE57, 기상청 API허브 수치모델 자료). "
    "kma-kim-relay에서 재가공: 동아시아 영역을 1/12° 격자에서 3칸 간격(0.25°)으로 추출하고 "
    "16비트로 양자화해 압축했습니다. 원자료와 다르며 공식 예보가 아닙니다."
)
REPO = os.environ.get("GITHUB_REPOSITORY", "illegal-username/kma-kim-relay")
INDEX_TAG = "index"
KEEP_CYCLES = 3
WORKERS = 6
# A stalled KMA request usually answers at once when sent again, so attempts start short.
ATTEMPT_TIMEOUTS = (8, 15, 30, 45)
# Pauses before each round over the fields of a lead still missing (the first is the pass itself).
RETRY_ROUND_PAUSES = (0, 15, 45, 90)
# Fields KMA still has not given this long after the cycle are given up, so the cycle can
# be marked complete (the app shows the gap) rather than block the app on older data.
GIVE_UP_HOURS = 10
AVAILABILITY_LAG_HOURS = 3
PROBE_CANDIDATES = 3


def log(*args):
    print(time.strftime("%H:%M:%S"), *args, flush=True)


def redact(text):
    return re.sub(r"(?i)(authkey\s*[=:]\s*)[^&\s<>]+", r"\1[REDACTED]", text)


class Missing(Exception):
    """KMA has not produced this cycle or lead yet."""


class Failed(Exception):
    pass


# ------------------------------------------------------------------ KMA API

def fetch(key, cycle, lead, name, data, level, window=WINDOW, size=(WIDTH, HEIGHT)):
    params = dict(group="KIMG", nwp="NE57", data=data, name=name, map="S", sub=window, tmfc=cycle,
                  hf=lead, disp="B", help=0, level=level)
    url = URL + "?" + urlencode(params) + "&authKey=" + key
    last = "no attempt"
    for attempt, timeout in enumerate(ATTEMPT_TIMEOUTS):
        if attempt:
            time.sleep(1)
        try:
            with urlopen(Request(url, headers={"User-Agent": "kma-kim-relay"}), timeout=timeout) as response:
                body = response.read()
        except HTTPError as error:
            if error.code in (401, 403):
                raise Failed("API key rejected (HTTP %d)" % error.code) from None
            last = "HTTP %d" % error.code
            continue
        except Exception as error:  # noqa: BLE001 - timeouts and network errors are retried
            last = redact(type(error).__name__ + ": " + str(error))[:100]
            continue
        nx, ny = struct.unpack_from("<ii", body) if len(body) >= 8 else (0, 0)
        if (nx, ny) == size and len(body) == 8 + nx * ny * 4:
            return np.frombuffer(body, "<f4", offset=8).reshape(ny, nx)
        text = redact(body[:2000].decode("euc-kr", "replace"))
        if re.search(r"not exist", text, re.I):
            raise Missing(text.strip()[:160])
        last = "unexpected answer: " + text.strip()[:120]
    raise Failed(last)


def cycle_exists(key, cycle):
    try:
        fetch(key, cycle, 0, "hgt", "P", 500, window=PROBE_WINDOW, size=(1, 1))
        return True
    except Missing:
        return False


def candidate_cycles(now=None):
    newest = (now or datetime.now(timezone.utc)) - timedelta(hours=AVAILABILITY_LAG_HOURS)
    newest = newest.replace(minute=0, second=0, microsecond=0, hour=newest.hour - newest.hour % 6)
    return [(newest - timedelta(hours=6 * i)).strftime("%Y%m%d%H") for i in range(PROBE_CANDIDATES)]


# ------------------------------------------------------------------ KQ16 (FieldCodec.java)

KQ16_MAGIC = 0x3631514B
MISSING_Q = 0xFFFF
MAX_STEPS = 0xFFFE


def kq16_encode(grid, decimals):
    """FieldCodec.encode for a float32 grid (ny, nx)."""
    ny, nx = grid.shape
    v = grid.astype(np.float32)
    missing = ~np.isfinite(v) | (np.abs(v) >= 1e19) | (v <= -900)
    units = v.astype(np.float64) * (10.0 ** decimals)
    units[missing] = np.nan
    rounded = np.floor(units[~missing] + 0.5)  # Java Math.round
    lo, hi = (int(rounded.min()), int(rounded.max())) if rounded.size else (0, 0)
    step = 1
    while hi - lo > MAX_STEPS * step:
        step *= 2
    with np.errstate(invalid="ignore"):
        q = np.clip(np.floor((units - lo) / step + 0.5), 0, MAX_STEPS)
    q = np.where(missing, MISSING_Q, q).astype(np.int64)
    previous = np.concatenate([np.zeros((ny, 1), np.int64), q[:, :-1]], axis=1)
    delta = ((q - previous) & 0xFFFF).astype(np.uint16).ravel()
    planes = np.concatenate([(delta >> 8).astype(np.uint8), (delta & 0xFF).astype(np.uint8)]).tobytes()
    header = struct.pack("<IBBhiiqi", KQ16_MAGIC, 1, decimals, 0, nx, ny, lo, step)
    return header + zlib.compress(planes, 6)


def kq16_decode(blob):
    magic, version, decimals, _, nx, ny, base, step = struct.unpack_from("<IBBhiiqi", blob)
    assert magic == KQ16_MAGIC and version == 1
    planes = np.frombuffer(zlib.decompress(blob[28:]), np.uint8)
    n = nx * ny
    delta = (planes[:n].astype(np.int64) << 8 | planes[n:]).reshape(ny, nx)
    q = np.cumsum(delta, axis=1) & 0xFFFF
    values = ((base + q * step) / 10.0 ** decimals).astype(np.float32)
    values[q == MISSING_Q] = np.nan
    return values


# ------------------------------------------------------------------ bundle

def build_bundle(cycle, lead, grids, missing):
    """KIMR: magic, format, 3 reserved bytes, header length, JSON header, KQ16 fields.

    `grids` are already thinned (every STRIDE-th point), as fetch_lead and read_bundle give them.
    """
    blobs, entries, offset = [], [], 0
    for name, _data, level, _required, decimals in FIELDS:
        grid = grids.get((name, level))
        if grid is None:
            continue
        blob = kq16_encode(grid, decimals)
        entries.append({"name": name, "level": level, "offset": offset, "length": len(blob)})
        blobs.append(blob)
        offset += len(blob)
    sub_ny, sub_nx = next(iter(grids.values())).shape
    header = {
        "format": FORMAT, "attribution": ATTRIBUTION, "model": "KIMG/NE57", "cycle": cycle, "lead": lead,
        "window": WINDOW, "stride": STRIDE, "nx": sub_nx, "ny": sub_ny, "levels": LEVELS,
        "fields": entries, "missing": missing,
        "created": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    encoded = json.dumps(header, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return b"KIMR" + struct.pack("<B3xI", FORMAT, len(encoded)) + encoded + b"".join(blobs)


def read_bundle(data):
    assert data[:4] == b"KIMR"
    _format, length = struct.unpack_from("<B3xI", data, 4)
    header = json.loads(data[12:12 + length].decode("utf-8"))
    body = 12 + length
    fields = {(f["name"], f["level"]): kq16_decode(data[body + f["offset"]:body + f["offset"] + f["length"]])
              for f in header["fields"]}
    return header, fields


def fetch_lead(key, cycle, lead, have=None):
    """
    The fields of one lead (thinned) on top of `have`, the ones already published. Fields
    that fail are tried again in later rounds, a little apart, since KMA stalls come and go.
    Raises Missing if KMA has not produced the lead yet.
    """
    grids = dict(have or {})
    todo = [spec for spec in FIELDS if (spec[0], spec[2]) not in grids]
    if not todo:
        return grids, []
    fetch(key, cycle, lead, "hgt", "P", 500, window=PROBE_WINDOW, size=(1, 1))
    errors = {}
    lock = threading.Lock()

    def one(spec):
        name, data, level, _required, _decimals = spec
        try:
            grid = fetch(key, cycle, lead, name, data, level)[::STRIDE, ::STRIDE].copy()
            with lock:
                grids[(name, level)] = grid
                errors.pop((name, level), None)
        except (Missing, Failed) as error:
            with lock:
                errors[(name, level)] = str(error)

    for round_index, pause in enumerate(RETRY_ROUND_PAUSES):
        if round_index:
            log("+%dh: %d field(s) again in %d s (%s)" % (
                lead, len(todo), pause, ", ".join("%s %d" % (s[0], s[2]) for s in todo)))
            time.sleep(pause)
        with ThreadPoolExecutor(WORKERS) as pool:
            list(pool.map(one, todo))
        todo = [spec for spec in FIELDS if (spec[0], spec[2]) not in grids]
        if not todo:
            break
    required = [(name, level) for name, _d, level, req, _q in FIELDS if req and (name, level) not in grids]
    missing = ["%s %d" % (name, level) for name, _d, level, _r, _q in FIELDS if (name, level) not in grids]
    if required:
        raise Failed("required fields missing: %s (%s)" % (
            ", ".join("%s %d" % item for item in required), errors.get(required[0], "")))
    return grids, missing


def published_fields(tag, file):
    """The fields of a bundle already on the release (KQ16 decodes and re-encodes exactly)."""
    url = "https://github.com/%s/releases/download/%s/%s" % (REPO, tag, file)
    try:
        with urlopen(Request(url, headers={"User-Agent": "kma-kim-relay"}), timeout=60) as response:
            return read_bundle(response.read())[1]
    except Exception as error:  # noqa: BLE001 - fetch everything again instead
        log("could not reuse %s: %s" % (file, error))
        return {}


# ------------------------------------------------------------------ GitHub release storage

def gh(*args, check=True):
    return subprocess.run(["gh", *args, "-R", REPO], check=check, capture_output=True, text=True)


def load_manifest(name="manifest.json", attribution=ATTRIBUTION):
    with tempfile.TemporaryDirectory() as folder:
        result = gh("release", "download", INDEX_TAG, "-p", name, "-D", folder, check=False)
        path = Path(folder) / name
        if result.returncode or not path.exists():
            return {"format": FORMAT, "attribution": attribution, "cycles": []}
        return json.loads(path.read_text(encoding="utf-8"))


def ensure_release(tag, title, notes):
    if gh("release", "view", tag, check=False).returncode:
        gh("release", "create", tag, "--title", title, "--notes", notes, "--latest=false")


def publish(manifest, folder, uploads, dry_run, name="manifest.json"):
    """Upload the files, then the manifest `name` (KIM: manifest.json) to the release "index"."""
    manifest["updated"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    path = Path(folder) / name
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    if dry_run:
        log("dry run: would upload", [Path(p).name for _t, p in uploads], "and", name)
        return
    for tag, file in uploads:
        gh("release", "upload", tag, file, "--clobber")
    ensure_release(INDEX_TAG, "Relay index", "manifest.json: 출처 기상청 KIM (재가공) · gfs-manifest.json: NOAA GFS")
    gh("release", "upload", INDEX_TAG, str(path), "--clobber")


def cycle_age_hours(cycle):
    start = datetime.strptime(cycle, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - start).total_seconds() / 3600


def is_complete(entry):
    """Every lead is published with every field, or the rest are given up (GIVE_UP_HOURS)."""
    leads = entry["leads"]
    if any(str(lead) not in leads for lead in LEADS):
        return False
    return cycle_age_hours(entry["cycle"]) > GIVE_UP_HOURS or not any(item["missing"] for item in leads.values())


def drop_old_cycles(manifest, dry_run):
    """Keep the cycles from the newest down to the newest complete one (the one the app shows)."""
    ordered = sorted(manifest["cycles"], key=lambda c: c["cycle"], reverse=True)
    keep = []
    for entry in ordered:
        keep.append(entry)
        if entry.get("complete"):
            break
    keep = keep[:KEEP_CYCLES]
    for entry in ordered:
        if entry not in keep and not dry_run:
            gh("release", "delete", entry["tag"], "--yes", "--cleanup-tag", check=False)
    manifest["cycles"] = keep


# ------------------------------------------------------------------ main

def run(key, cycle, leads, out_dir, dry_run, time_budget):
    started = time.time()
    manifest = load_manifest()
    entry = next((c for c in manifest["cycles"] if c["cycle"] == cycle), None)
    if entry is None:
        entry = {"cycle": cycle, "tag": "kim-" + cycle, "leads": {}}
        manifest["cycles"].append(entry)
    # Bundles of an older field list (no "fields" count) get the new fields added too.
    todo = [lead for lead in leads if str(lead) not in entry["leads"] or entry["leads"][str(lead)]["missing"]
            or entry["leads"][str(lead)].get("fields", 0) < len(FIELDS)]
    log("cycle", cycle, "to fetch:", todo)
    if not todo:
        return 0
    if not dry_run:
        ensure_release(entry["tag"], "KIM " + cycle, ATTRIBUTION)
    done = 0
    for lead in todo:
        if time.time() - started > time_budget:
            log("time budget used up; the next run continues")
            break
        lead_started = time.time()
        previous = entry["leads"].get(str(lead))
        # A bundle published with gaps: keep its fields and fetch only the missing ones.
        have = published_fields(entry["tag"], previous["file"]) if previous and not dry_run else {}
        try:
            grids, missing = fetch_lead(key, cycle, lead, have)
        except Missing:
            log("+%dh not produced yet" % lead)
            break
        except Failed as error:
            log("+%dh failed:" % lead, error)
            continue
        if previous and len(grids) == len(have):
            log("+%dh still missing %s; kept the published bundle" % (lead, missing))
            continue
        file = Path(out_dir) / ("kim-%s-f%03d.kimr" % (cycle, lead))
        file.write_bytes(build_bundle(cycle, lead, grids, missing))
        entry["leads"][str(lead)] = {"file": file.name, "bytes": file.stat().st_size, "missing": missing,
                                     "fields": len(FIELDS)}
        # The app switches to a cycle only once it is complete, so a lead published now is
        # used when the last one is in.
        entry["complete"] = is_complete(entry)
        drop_old_cycles(manifest, dry_run)
        publish(manifest, out_dir, [(entry["tag"], str(file))], dry_run)
        done += 1
        log("+%dh published: %d bytes, %.0f s%s" % (lead, file.stat().st_size, time.time() - lead_started,
                                                   ", missing " + ", ".join(missing) if missing else ""))
    if not entry.get("complete") and is_complete(entry):
        # Gaps given up on with time: mark complete without a new bundle.
        entry["complete"] = True
        drop_old_cycles(manifest, dry_run)
        publish(manifest, out_dir, [], dry_run)
    log("cycle %s %s" % (cycle, "complete" if entry.get("complete") else "not complete yet"))
    return done


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cycle", help="YYYYMMDDHH; default: the newest KIM cycle")
    parser.add_argument("--leads", help="comma separated; default: 0-48 h every 3 h")
    parser.add_argument("--out", help="keep the bundles in this folder")
    parser.add_argument("--dry-run", action="store_true", help="do not touch GitHub releases")
    parser.add_argument("--time-budget", type=float, default=40 * 60, help="seconds")
    args = parser.parse_args()
    key = os.environ.get("KMA_API_KEY", "").strip()
    if not key:
        sys.exit("KMA_API_KEY is not set")
    leads = [int(item) for item in args.leads.split(",")] if args.leads else LEADS
    cycle = args.cycle
    if not cycle:
        cycle = next((c for c in candidate_cycles() if cycle_exists(key, c)), None)
        if not cycle:
            log("no recent KIM cycle answered")
            return
    out_dir = args.out or tempfile.mkdtemp(prefix="kimr-")
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    try:
        log("published %d lead(s)" % run(key, cycle, leads, out_dir, args.dry_run, args.time_budget))
    finally:
        if not args.out:
            shutil.rmtree(out_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
