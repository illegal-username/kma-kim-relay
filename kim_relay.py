"""Pre-fetch KMA KIM forecast fields for East Asia and publish them as compact bundles.

Fields are fetched as GRIB2 from KMA's lightweight API (25% less volume than the older
binary API, same values), and fetching stops for the day at DAILY_BUDGET (4.9 GB, KMA allows
5 GB a day), counted in manifest.json and reset at midnight KST.

Each run looks for the newest KIM cycle, fetches the leads that are not published yet
(or were published with fields missing, of which only the missing ones are fetched again)
and uploads one bundle per lead to the GitHub release "kim-<cycle>", then rewrites
manifest.json, the only file of the branch "manifest-kim". Bundle names carry a hash of
their content and are never overwritten, and the manifest lives in git rather than as a
release asset: replacing a release asset leaves its URL answering 404 for a while. Fields that fail are retried in a few rounds within
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
import hashlib
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

# Fields come as GRIB2 from the "lightweight" API: 24-bit packing, 25% smaller than the float32
# of the older API with the same values (checked to 3e-5 K on 2026-10-26), which keeps four
# cycles a day under KMA's daily volume limit. The older API stays for the tiny existence probes.
URL = "https://apihub.kma.go.kr/api/typ06/cgi-bin/url/nph-kim_nc_xy_txt2_std"
GRIB_URL = "https://apihub.kma.go.kr/api/typ06/url/nwp_nc_vars_down.php"
# 100-160E, 5-55N of the g576 grid (1-based x_min,y_min,x_max,y_max): the app's window.
WINDOW = "1201,1141,1921,1741"
# The same 721x601 window by longitude/latitude; 55.05 so the northern row (55.04N) is in.
BOX = dict(lon1=100, lat1=5, lon2=160, lat2=55.05)
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
INDEX_TAG = "index"  # held the manifests before 2026-10-23; no longer updated
# Each manifest is the only file of its own branch (force-pushed), read by the app from
# raw.githubusercontent.com.
MANIFEST_BRANCHES = {"manifest.json": "manifest-kim", "gfs-manifest.json": "manifest-gfs"}
# Manifests read from the old release asset, to be pushed to their branch once.
UNMIGRATED = set()
KEEP_CYCLES = 3
WORKERS = 6
# A stalled KMA request usually answers at once when sent again, so attempts start short.
ATTEMPT_TIMEOUTS = (8, 15, 30, 45)
# The GRIB2 API answers within a second or not at all: about half the requests stall (its
# gateway gives 504 after 30 s), and the same request sent again usually answers at once.
# Waiting 30 s on each stall took 350-520 s a lead; 5 s attempts took 70 s (2026-10-26).
GRIB_ATTEMPT_TIMEOUTS = (5, 5, 5, 5, 5, 10, 10, 20, 30)
# KMA API허브 lets a general member receive 5 GB a day (reset at midnight KST); a 403 says
# "일일 최대 호출 용량 제한" once it is used. The relay stops below that, at 4.9 GB, and the
# next run after midnight KST goes on. The count is kept in manifest.json ("usage").
DAILY_BUDGET = 4.9e9
FIELD_BYTES = 1300142  # one GRIB2 field of the window
# Pauses before each round over the fields of a lead still missing (the first is the pass itself).
RETRY_ROUND_PAUSES = (0, 15, 45, 90)
# Fields KMA still has not given this long after the cycle are given up, so the cycle can
# be marked complete (the app shows the gap) rather than block the app on older data.
GIVE_UP_HOURS = 10
AVAILABILITY_LAG_HOURS = 3
# Times already past are of no use: the app shows a cycle from the time nearest to now on.
# A new cycle is fetched from the time nearest to an hour from now (it is complete and shown
# about then), which skips +0 h and +3 h of a cycle found 4-5 h after it ran.
FIRST_LEAD_AHEAD_HOURS = 1
PROBE_CANDIDATES = 3


def log(*args):
    print(time.strftime("%H:%M:%S"), *args, flush=True)


def redact(text):
    return re.sub(r"(?i)(authkey\s*[=:]\s*)[^&\s<>]+", r"\1[REDACTED]", text)


class Missing(Exception):
    """KMA has not produced this cycle or lead yet."""


class Failed(Exception):
    pass


class OverBudget(Exception):
    """Today's KMA volume (DAILY_BUDGET, or KMA's own limit) is used up."""


# ------------------------------------------------------------------ daily volume

def kst_day():
    return (datetime.now(timezone.utc) + timedelta(hours=9)).strftime("%Y-%m-%d")


USAGE = {"day": kst_day(), "bytes": 0}
USAGE_LOCK = threading.Lock()


def load_usage(manifest):
    saved = manifest.get("usage") or {}
    with USAGE_LOCK:
        if saved.get("day") == kst_day():
            USAGE.update(day=saved["day"], bytes=int(saved.get("bytes", 0)))


def count_bytes(n=0, exhausted=False):
    """Add n received bytes to today's count; `exhausted` (KMA said so) fills the day."""
    with USAGE_LOCK:
        if USAGE["day"] != kst_day():
            USAGE.update(day=kst_day(), bytes=0)
        USAGE["bytes"] += n
        if exhausted:
            USAGE["bytes"] = max(USAGE["bytes"], int(DAILY_BUDGET))


def check_budget(expected=FIELD_BYTES):
    count_bytes()
    if USAGE["bytes"] + expected >= DAILY_BUDGET:
        raise OverBudget("%.2f GB of the daily %.1f GB used" % (USAGE["bytes"] / 1e9, DAILY_BUDGET / 1e9))


# ------------------------------------------------------------------ KMA API

def http_failure(error):
    """Raise for a 401/403: KMA's daily limit (OverBudget) or a rejected key (Failed)."""
    if error.code not in (401, 403):
        return
    try:
        message = error.read()[:500].decode("utf-8", "replace")
    except Exception:  # noqa: BLE001
        message = ""
    if "제한" in message or "limit" in message.lower():
        count_bytes(exhausted=True)
        raise OverBudget("KMA daily limit reached (HTTP %d)" % error.code) from None
    raise Failed("API key rejected (HTTP %d)" % error.code) from None


def fetch(key, cycle, lead, name, data, level, window=WINDOW, size=(WIDTH, HEIGHT)):
    """A field from the older binary API (now only the 1x1 probes)."""
    params = dict(group="KIMG", nwp="NE57", data=data, name=name, map="S", sub=window, tmfc=cycle,
                  hf=lead, disp="B", help=0, level=level)
    url = URL + "?" + urlencode(params) + "&authKey=" + key
    last = "no attempt"
    for attempt, timeout in enumerate(ATTEMPT_TIMEOUTS):
        if attempt:
            time.sleep(1)
        check_budget(0)
        try:
            with urlopen(Request(url, headers={"User-Agent": "kma-kim-relay"}), timeout=timeout) as response:
                body = response.read()
        except HTTPError as error:
            http_failure(error)
            last = "HTTP %d" % error.code
            continue
        except Exception as error:  # noqa: BLE001 - timeouts and network errors are retried
            last = redact(type(error).__name__ + ": " + str(error))[:100]
            continue
        count_bytes(len(body))
        nx, ny = struct.unpack_from("<ii", body) if len(body) >= 8 else (0, 0)
        if (nx, ny) == size and len(body) == 8 + nx * ny * 4:
            return np.frombuffer(body, "<f4", offset=8).reshape(ny, nx)
        text = redact(body[:2000].decode("euc-kr", "replace"))
        if re.search(r"not exist", text, re.I):
            raise Missing(text.strip()[:160])
        last = "unexpected answer: " + text.strip()[:120]
    raise Failed(last)


def grib_values(body):
    """The grid of a one-message GRIB2 (template 3.0, simple packing 5.0) as float32 (nj, ni),
    south row first. KMA labels every variable as temperature (category 0, parameter 0), so
    the message is read as the field that was asked for."""
    if body[:4] != b"GRIB" or body[7] != 2:
        raise ValueError("not GRIB2")
    pos, ni, nj, packing, bitmap, packed = 16, 0, 0, None, None, None
    while pos + 5 <= len(body) and body[pos:pos + 4] != b"7777":
        length, number = struct.unpack_from(">IB", body, pos)
        section = body[pos:pos + length]
        if number == 3:
            ni, nj = struct.unpack_from(">II", section, 30)
            if not section[71] & 0x40:
                raise ValueError("rows run north to south")
        elif number == 5:
            if struct.unpack_from(">H", section, 9)[0] != 0:
                raise ValueError("packing template %d" % struct.unpack_from(">H", section, 9)[0])
            ref = struct.unpack_from(">f", section, 11)[0]
            e, d = struct.unpack_from(">HH", section, 15)
            e = -(e & 0x7FFF) if e & 0x8000 else e
            d = -(d & 0x7FFF) if d & 0x8000 else d
            packing = ref, e, d, section[19]
        elif number == 6 and section[5] == 0:
            bitmap = np.unpackbits(np.frombuffer(section, np.uint8, offset=6))
        elif number == 7:
            packed = np.frombuffer(section, np.uint8, offset=5)
        pos += length
    if not ni or packing is None or packed is None:
        raise ValueError("incomplete GRIB2")
    ref, e, d, bits = packing
    count = int(bitmap[:ni * nj].sum()) if bitmap is not None else ni * nj
    if bits:
        digits = np.unpackbits(packed)[:count * bits].reshape(count, bits).astype(np.int64)
        scaled = digits.dot(1 << np.arange(bits - 1, -1, -1, dtype=np.int64))
    else:
        scaled = np.zeros(count, np.int64)
    values = ((ref + scaled * 2.0 ** e) / 10.0 ** d).astype(np.float32)
    if bitmap is not None:
        grid = np.full(ni * nj, np.nan, np.float32)
        grid[bitmap[:ni * nj].astype(bool)] = values
        values = grid
    return values.reshape(nj, ni)


def fetch_grib(key, cycle, lead, name, data, level, stats=None):
    """A field of the window from the GRIB2 API, as the older API's (601, 721) float32.
    Each attempt is added to `stats` as (outcome, seconds to the answer's headers)."""
    params = dict(group="KIMG", nwp="ne57", data=data, vars=name, tmfc=cycle, hf=lead,
                  dataType="GB2", help=0, **BOX)
    if data == "P":
        params["lvl_lst"] = level
    url = GRIB_URL + "?" + urlencode(params) + "&authKey=" + key
    last = "no attempt"
    for attempt, timeout in enumerate(GRIB_ATTEMPT_TIMEOUTS):
        if attempt:
            time.sleep(1)
        check_budget()
        sent = time.time()
        outcome = "ok"
        try:
            with urlopen(Request(url, headers={"User-Agent": "kma-kim-relay"}), timeout=timeout) as response:
                waited = time.time() - sent
                body = response.read()
        except HTTPError as error:
            outcome = "HTTP %d" % error.code
            http_failure(error)
            last = outcome
            continue
        except Exception as error:  # noqa: BLE001 - timeouts and network errors are retried
            outcome = "stall" if "timed out" in str(error) else type(error).__name__
            last = redact(type(error).__name__ + ": " + str(error))[:100]
            continue
        finally:
            if stats is not None:
                stats.append((outcome, waited if outcome == "ok" else time.time() - sent))
        count_bytes(len(body))
        if body[:4] == b"GRIB":
            try:
                grid = grib_values(body)
            except (ValueError, IndexError, struct.error) as error:
                last = "bad GRIB2: %s" % error
                continue
            if grid.shape == (HEIGHT, WIDTH):
                return grid
            last = "unexpected grid %dx%d" % (grid.shape[1], grid.shape[0])
            continue
        text = redact(body[:2000].decode("utf-8", "replace"))
        # "# ERROR: 파일 없음 (/ARCV/.../g576_..._prs.2byte.ft000.<cycle>.nc)"
        if re.search(r"ERROR[^\n]*\(/[^)\n]*\.nc\)", text):
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


def log_attempts(lead, stats):
    """One line on how KMA answered a lead's requests: where its time goes."""
    if not stats:
        return
    answered = sorted(seconds for outcome, seconds in stats if outcome == "ok")
    failed = {}
    for outcome, seconds in stats:
        if outcome != "ok":
            failed.setdefault(outcome, []).append(seconds)
    waits = ", ".join("%s %d (%.0f s)" % (outcome, len(times), sum(times)) for outcome, times in sorted(failed.items()))
    log("+%dh requests: %d, answered %d (median %.1f s, max %.1f s, %.0f s in all)%s" % (
        lead, len(stats), len(answered), answered[len(answered) // 2] if answered else 0,
        answered[-1] if answered else 0, sum(answered), "; " + waits if waits else ""))


def fetch_lead(key, cycle, lead, have=None):
    """
    The fields of one lead (thinned) on top of `have`, the ones already published. Fields
    that fail are tried again in later rounds, a little apart, since KMA stalls come and go.
    Raises Missing if KMA has not produced the lead yet, OverBudget once today's volume is used.
    """
    grids = dict(have or {})
    todo = [spec for spec in FIELDS if (spec[0], spec[2]) not in grids]
    if not todo:
        return grids, []
    fetch(key, cycle, lead, "hgt", "P", 500, window=PROBE_WINDOW, size=(1, 1))
    errors = {}
    over = []
    stats = []
    lock = threading.Lock()

    def one(spec):
        name, data, level, _required, _decimals = spec
        try:
            grid = fetch_grib(key, cycle, lead, name, data, level, stats)[::STRIDE, ::STRIDE].copy()
            with lock:
                grids[(name, level)] = grid
                errors.pop((name, level), None)
        except (Missing, Failed) as error:
            with lock:
                errors[(name, level)] = str(error)
        except OverBudget as error:
            over.append(error)

    try:
        for round_index, pause in enumerate(RETRY_ROUND_PAUSES):
            if round_index:
                log("+%dh: %d field(s) again in %d s (%s)" % (
                    lead, len(todo), pause, ", ".join("%s %d" % (s[0], s[2]) for s in todo)))
                time.sleep(pause)
            with ThreadPoolExecutor(WORKERS) as pool:
                list(pool.map(one, todo))
            if over:
                raise over[0]
            todo = [spec for spec in FIELDS if (spec[0], spec[2]) not in grids]
            if not todo:
                break
    finally:
        log_attempts(lead, stats)
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
    # From the git API (no CDN delay), else the release asset of older runs.
    result = subprocess.run(["gh", "api", "-H", "Accept: application/vnd.github.raw",
                             "repos/%s/contents/%s?ref=%s" % (REPO, name, MANIFEST_BRANCHES[name])],
                            capture_output=True, text=True, encoding="utf-8")
    if result.returncode == 0 and result.stdout.startswith("{"):
        return json.loads(result.stdout)
    UNMIGRATED.add(name)
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
    """Upload the files, then the manifest `name` (KIM: manifest.json) to its branch."""
    manifest["updated"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    path = Path(folder) / name
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    if dry_run:
        log("dry run: would upload", [Path(p).name for _t, p in uploads], "and", name)
        return
    for tag, file in uploads:
        gh("release", "upload", tag, file, "--clobber")
    push_manifest(path, name)


def push_manifest(path, name):
    """Replace the branch of manifest `name` with one commit holding just that file."""
    branch = MANIFEST_BRANCHES[name]
    token = os.environ.get("GH_TOKEN", "")
    url = ("https://x-access-token:%s@github.com/%s.git" % (token, REPO)) if token else "https://github.com/%s.git" % REPO
    with tempfile.TemporaryDirectory() as repo:
        shutil.copy(path, Path(repo) / name)
        steps = [
            ["init", "-q", "-b", branch],
            ["add", name],
            ["-c", "user.name=github-actions[bot]", "-c", "user.email=41898282+github-actions[bot]@users.noreply.github.com",
             "commit", "-q", "-m", "Update " + name],
            ["push", "-q", "--force", url, "HEAD:refs/heads/" + branch],
        ]
        for step in steps:
            result = subprocess.run(["git", *step], cwd=repo, capture_output=True, text=True)
            if result.returncode:
                # The push URL holds the token: report the step, not the command.
                raise RuntimeError("git %s failed: %s" % (step[0] if step[0] != "-c" else "commit",
                                                         result.stderr.replace(token, "***") if token else result.stderr))


def cycle_age_hours(cycle):
    start = datetime.strptime(cycle, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - start).total_seconds() / 3600


def first_lead(cycle):
    """The lead nearest to FIRST_LEAD_AHEAD_HOURS from now (the last one once all are past)."""
    steps = int((cycle_age_hours(cycle) + FIRST_LEAD_AHEAD_HOURS) / 3 + 0.5)
    return LEADS[max(0, min(len(LEADS) - 1, steps))]


def is_complete(entry):
    """Every lead from the entry's first is published with every field, or the rest are given
    up (GIVE_UP_HOURS)."""
    leads = entry["leads"]
    if any(str(lead) not in leads for lead in LEADS if lead >= entry.get("first", 0)):
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

def usage_unsaved(manifest):
    """More than the probes' few bytes received since manifest.json last had the count."""
    saved = manifest.get("usage") or {}
    with USAGE_LOCK:
        return saved.get("day") != USAGE["day"] or USAGE["bytes"] - saved.get("bytes", 0) > 1e6


def publish_kim(manifest, folder, uploads, dry_run):
    """publish() of manifest.json with today's KMA volume in it, for the next run to go on from."""
    with USAGE_LOCK:
        manifest["usage"] = dict(USAGE)
    publish(manifest, folder, uploads, dry_run)


def run(key, manifest, cycle, leads, out_dir, dry_run, time_budget, skip_past=True):
    started = time.time()
    entry = next((c for c in manifest["cycles"] if c["cycle"] == cycle), None)
    if entry is None:
        # The leads before "first" are never fetched for this cycle (the app starts there).
        entry = {"cycle": cycle, "tag": "kim-" + cycle, "first": first_lead(cycle) if skip_past else 0, "leads": {}}
        manifest["cycles"].append(entry)
    # Bundles of an older field list (no "fields" count) get the new fields added too.
    todo = [lead for lead in leads if (not skip_past or lead >= entry.get("first", 0))
            and (str(lead) not in entry["leads"] or entry["leads"][str(lead)]["missing"]
                 or entry["leads"][str(lead)].get("fields", 0) < len(FIELDS))]
    log("cycle", cycle, "to fetch:", todo)
    if not todo:
        if "manifest.json" in UNMIGRATED:
            publish_kim(manifest, out_dir, [], dry_run)
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
        except OverBudget as error:
            log("+%dh stopped: %s; the next run after midnight KST goes on" % (lead, error))
            break
        except Failed as error:
            log("+%dh failed:" % lead, error)
            continue
        if previous and len(grids) == len(have):
            log("+%dh still missing %s; kept the published bundle" % (lead, missing))
            continue
        data = build_bundle(cycle, lead, grids, missing)
        # A new name for every version: an app that read the previous manifest still finds its file.
        file = Path(out_dir) / ("kim-%s-f%03d-%s.kimr" % (cycle, lead, hashlib.sha256(data).hexdigest()[:8]))
        file.write_bytes(data)
        entry["leads"][str(lead)] = {"file": file.name, "bytes": file.stat().st_size, "missing": missing,
                                     "fields": len(FIELDS)}
        # The app switches to a cycle only once it is complete, so a lead published now is
        # used when the last one is in.
        entry["complete"] = is_complete(entry)
        drop_old_cycles(manifest, dry_run)
        publish_kim(manifest, out_dir, [(entry["tag"], str(file))], dry_run)
        done += 1
        log("+%dh published: %d bytes, %.0f s%s" % (lead, file.stat().st_size, time.time() - lead_started,
                                                   ", missing " + ", ".join(missing) if missing else ""))
    if not entry.get("complete") and is_complete(entry):
        # Gaps given up on with time: mark complete without a new bundle.
        entry["complete"] = True
        drop_old_cycles(manifest, dry_run)
        publish_kim(manifest, out_dir, [], dry_run)
    elif ("manifest.json" in UNMIGRATED and not done) or usage_unsaved(manifest):
        # Keep the count of fields fetched in vain too (failed leads, OverBudget mid-lead).
        publish_kim(manifest, out_dir, [], dry_run)
    log("cycle %s %s" % (cycle, "complete" if entry.get("complete") else "not complete yet"))
    return done


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cycle", help="YYYYMMDDHH; default: the newest KIM cycle")
    parser.add_argument("--leads", help="comma separated; default: 0-48 h every 3 h")
    parser.add_argument("--out", help="keep the bundles in this folder")
    parser.add_argument("--dry-run", action="store_true", help="do not touch GitHub releases")
    parser.add_argument("--time-budget", type=float, default=45 * 60, help="seconds")
    args = parser.parse_args()
    key = os.environ.get("KMA_API_KEY", "").strip()
    if not key:
        sys.exit("KMA_API_KEY is not set")
    leads = [int(item) for item in args.leads.split(",")] if args.leads else LEADS
    manifest = load_manifest()
    load_usage(manifest)
    log("KMA volume today (KST %s): %.2f GB of %.1f GB" % (USAGE["day"], USAGE["bytes"] / 1e9, DAILY_BUDGET / 1e9))
    try:
        check_budget()
    except OverBudget as error:
        log("nothing fetched: %s; the next run after midnight KST goes on" % error)
        return
    cycle = args.cycle
    if not cycle:
        cycle = None
        for candidate in candidate_cycles():
            try:
                if cycle_exists(key, candidate):
                    cycle = candidate
                    break
            except Failed as error:
                # KMA stalled even on the tiny probe: an older cycle is checked, this one next run.
                log("could not tell whether %s exists: %s" % (candidate, error))
            except OverBudget as error:
                log("stopped: %s; the next run after midnight KST goes on" % error)
                if usage_unsaved(manifest):
                    with tempfile.TemporaryDirectory() as folder:
                        publish_kim(manifest, folder, [], args.dry_run)
                return
        if not cycle:
            log("no recent KIM cycle answered")
            return
    out_dir = args.out or tempfile.mkdtemp(prefix="kimr-")
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    try:
        # Leads asked for by name are fetched even if past.
        log("published %d lead(s)" % run(key, manifest, cycle, leads, out_dir, args.dry_run, args.time_budget,
                                         skip_past=not args.leads))
        log("KMA volume today: %.2f GB" % (USAGE["bytes"] / 1e9))
    finally:
        if not args.out:
            shutil.rmtree(out_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
