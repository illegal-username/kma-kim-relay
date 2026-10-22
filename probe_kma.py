"""Can a GitHub Actions runner use the KMA API? Prints timings only, never the key.

Without KMA_API_KEY it only checks that apihub.kma.go.kr answers at all (an error text
is fine); with it, it times a probe and a few full KIM fields, serially and in parallel.
"""
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

URL = "https://apihub.kma.go.kr/api/typ06/cgi-bin/url/nph-kim_nc_xy_txt2_std"
WINDOW = "1201,1141,1921,1741"
KEY = os.environ.get("KMA_API_KEY", "").strip()


def redact(text):
    return re.sub(r"(?i)(authkey\s*[=:]\s*)[^&\s<>]+", r"\1[REDACTED]", text)


def get(params, key=KEY, timeout=45):
    query = urlencode(params) + (("&authKey=" + key) if key else "")
    start = time.time()
    result = {}
    try:
        with urlopen(Request(URL + "?" + query, headers={"User-Agent": "kma3d-relay-probe"}), timeout=timeout) as response:
            first = response.read(1)
            result["ttfb"] = round(time.time() - start, 2)
            body = first + response.read()
            result["status"] = response.status
    except HTTPError as error:
        body = error.read()
        result["status"] = error.code
    except Exception as error:  # noqa: BLE001 - report every failure kind
        body = b""
        result["error"] = redact(type(error).__name__ + ": " + str(error))[:120]
    result["sec"] = round(time.time() - start, 2)
    result["bytes"] = len(body)
    if body and len(body) < 2000:
        result["text"] = redact(body.decode("euc-kr", "replace")).strip()[:160]
    return result


def params(cycle, lead=0, name="hgt", level=500, data="P", window=WINDOW):
    return dict(group="KIMG", nwp="NE57", data=data, name=name, map="S", sub=window, tmfc=cycle,
                hf=lead, disp="B", help=0, level=level)


def candidate_cycles(count=3):
    newest = datetime.now(timezone.utc) - timedelta(hours=4)
    newest = newest.replace(minute=0, second=0, microsecond=0, hour=newest.hour - newest.hour % 6)
    return [(newest - timedelta(hours=6 * i)).strftime("%Y%m%d%H") for i in range(count)]


def main():
    cycles = candidate_cycles()
    print("runner time", datetime.now(timezone.utc).isoformat(timespec="seconds"), "candidates", cycles)
    print("no key:", get(params(cycles[-1], window="1201,1141,1201,1141"), key=""))
    if not KEY:
        print("KMA_API_KEY secret is not set: only reachability was checked.")
        return
    cycle = None
    for candidate in cycles:
        result = get(params(candidate, window="1201,1141,1201,1141"))
        print("probe", candidate, result)
        if result.get("bytes") == 12:
            cycle = candidate
            break
    if not cycle:
        print("no cycle answered")
        return
    levels = [1000, 850, 700, 500, 300, 200]
    start = time.time()
    for level in levels:
        print("serial hgt", level, get(params(cycle, level=level)))
    print("serial total", round(time.time() - start, 1))
    start = time.time()
    specs = [("T", level) for level in levels] + [("u", 500), ("v", 500)]
    with ThreadPoolExecutor(8) as pool:
        results = list(pool.map(lambda spec: (spec, get(params(cycle, name=spec[0], level=spec[1]))), specs))
    for spec, result in results:
        print("parallel", spec, result)
    print("parallel total", round(time.time() - start, 1))


if __name__ == "__main__":
    main()
