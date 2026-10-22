"""Pre-fetch KMA KIM forecast fields for East Asia and publish them as compact bundles.

Each run looks for the newest KIM cycle, fetches the leads that are not published yet
(or were published with fields missing) and uploads one bundle per lead to the GitHub
release "kim-<cycle>", then rewrites manifest.json on the release "index". Only the two
newest cycles are kept.

A bundle ("KIMR") holds the 38 fields of one lead -- height, temperature and wind on 9
pressure levels, MSLP and surface pressure -- on every third point of the app's 721x601
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
)
FORMAT = 1
ATTRIBUTION = (
    "출처: 기상청 (KIM 전구모델 KIMG/NE57, 기상청 API허브 수치모델 자료). "
    "kma-kim-relay에서 재가공: 동아시아 영역을 1/12° 격자에서 3칸 간격(0.25°)으로 추출하고 "
    "16비트로 양자화해 압축했습니다. 원자료와 다르며 공식 예보가 아닙니다."
)
REPO = os.environ.get("GITHUB_REPOSITORY", "illegal-username/kma-kim-relay")
INDEX_TAG = "index"
KEEP_CYCLES = 2
WORKERS = 6
# A stalled KMA request usually answers at once when sent again, so attempts start short.
ATTEMPT_TIMEOUTS = (8, 15, 30, 45)
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
    """KIMR: magic, format, 3 reserved bytes, header length, JSON header, KQ16 fields."""
    blobs, entries, offset = [], [], 0
    for name, _data, level, _required, decimals in FIELDS:
        grid = grids.get((name, level))
        if grid is None:
            continue
        blob = kq16_encode(grid[::STRIDE, ::STRIDE], decimals)
        entries.append({"name": name, "level": level, "offset": offset, "length": len(blob)})
        blobs.append(blob)
        offset += len(blob)
    sub_ny, sub_nx = next(iter(grids.values()))[::STRIDE, ::STRIDE].shape
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


def fetch_lead(key, cycle, lead):
    """All fields of one lead; raises Missing if KMA has not produced the lead yet."""
    fetch(key, cycle, lead, "hgt", "P", 500, window=PROBE_WINDOW, size=(1, 1))
    grids, errors = {}, {}
    lock = threading.Lock()

    def one(spec):
        name, data, level, _required, _decimals = spec
        try:
            grid = fetch(key, cycle, lead, name, data, level)
            with lock:
                grids[(name, level)] = grid
        except (Missing, Failed) as error:
            with lock:
                errors[(name, level)] = str(error)

    with ThreadPoolExecutor(WORKERS) as pool:
        list(pool.map(one, FIELDS))
    required = [(name, level) for name, _d, level, req, _q in FIELDS if req and (name, level) not in grids]
    missing = ["%s %d" % (name, level) for name, _d, level, _r, _q in FIELDS if (name, level) not in grids]
    if required:
        raise Failed("required fields missing: %s (%s)" % (
            ", ".join("%s %d" % item for item in required), errors[required[0]]))
    return grids, missing


# ------------------------------------------------------------------ GitHub release storage

def gh(*args, check=True):
    return subprocess.run(["gh", *args, "-R", REPO], check=check, capture_output=True, text=True)


def load_manifest():
    with tempfile.TemporaryDirectory() as folder:
        result = gh("release", "download", INDEX_TAG, "-p", "manifest.json", "-D", folder, check=False)
        path = Path(folder) / "manifest.json"
        if result.returncode or not path.exists():
            return {"format": FORMAT, "attribution": ATTRIBUTION, "cycles": []}
        return json.loads(path.read_text(encoding="utf-8"))


def ensure_release(tag, title, notes):
    if gh("release", "view", tag, check=False).returncode:
        gh("release", "create", tag, "--title", title, "--notes", notes, "--latest=false")


def publish(manifest, folder, uploads, dry_run):
    manifest["updated"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    path = Path(folder) / "manifest.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    if dry_run:
        log("dry run: would upload", [Path(p).name for _t, p in uploads], "and manifest.json")
        return
    for tag, file in uploads:
        gh("release", "upload", tag, file, "--clobber")
    ensure_release(INDEX_TAG, "KIM relay index", ATTRIBUTION)
    gh("release", "upload", INDEX_TAG, str(path), "--clobber")


def drop_old_cycles(manifest, dry_run):
    keep = sorted(manifest["cycles"], key=lambda c: c["cycle"], reverse=True)[:KEEP_CYCLES]
    for entry in manifest["cycles"]:
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
    todo = [lead for lead in leads if str(lead) not in entry["leads"] or entry["leads"][str(lead)]["missing"]]
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
        try:
            grids, missing = fetch_lead(key, cycle, lead)
        except Missing:
            log("+%dh not produced yet" % lead)
            break
        except Failed as error:
            log("+%dh failed:" % lead, error)
            continue
        previous = entry["leads"].get(str(lead))
        if previous and len(missing) >= len(previous["missing"]):
            log("+%dh still missing %s; kept the published bundle" % (lead, missing))
            continue
        file = Path(out_dir) / ("kim-%s-f%03d.kimr" % (cycle, lead))
        file.write_bytes(build_bundle(cycle, lead, grids, missing))
        entry["leads"][str(lead)] = {"file": file.name, "bytes": file.stat().st_size, "missing": missing}
        # Publishing after every lead lets the app use the first leads while the rest arrive.
        drop_old_cycles(manifest, dry_run)
        publish(manifest, out_dir, [(entry["tag"], str(file))], dry_run)
        done += 1
        log("+%dh published: %d bytes, %.0f s%s" % (lead, file.stat().st_size, time.time() - lead_started,
                                                   ", missing " + ", ".join(missing) if missing else ""))
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
