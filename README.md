# KIM relay

기상청 KIM 전구 수치예보(KIMG/NE57)의 동아시아 영역(100–160°E, 5–55°N)을
생산 주기마다 미리 받아 가볍게 가공해 두는 저장소입니다. 개인용 Android 앱
「KMA 3D 종관 일기도」가 기상청 API를 직접 기다리지 않고 이 파일을 받아 씁니다.

**출처: 기상청** — 기상청 API허브 「수치모델 자료」의 KIM 전구모델 자료를 이 저장소에서 재가공했습니다
(1/12° 격자를 3칸 간격(0.25°)으로 추출하고 16비트로 양자화해 압축). 원자료는
[기상청 API허브](https://apihub.kma.go.kr)에서 받을 수 있습니다. 가공된 자료는 원자료와 다르며, 공식 예보가 아닙니다.

## 동작

- [relay.yml](.github/workflows/relay.yml)이 30분마다 [kim_relay.py](kim_relay.py)를 실행합니다.
- 가장 최신 KIM 주기에서 아직 올리지 않은 예보 시각(+0–48 h, 3시간 간격)을 받아
  릴리스 `kim-<주기>`에 시각마다 파일 하나로 올리고, 목록 `manifest.json`을 갱신합니다.
- 목록은 브랜치 `manifest-kim`(GFS는 `manifest-gfs`)의 유일한 파일이고, 앱은
  `https://raw.githubusercontent.com/illegal-username/kma-kim-relay/manifest-kim/manifest.json`에서 읽습니다.
  릴리스 첨부 파일을 덮어쓰면 그 주소가 한동안 404가 되기 때문입니다. 같은 이유로 중계 파일 이름에는 내용 해시가
  붙고(`kim-<주기>-fLLL-<해시>.kimr`) 한 번 올린 파일은 덮어쓰지 않습니다.
- 실패한 필드는 같은 실행 안에서 간격을 두고 몇 번 더 받고, 그래도 빠진 필드는 다음 실행이
  이미 올린 파일에 그 필드만 채워 다시 올립니다(나머지 필드는 다시 받지 않음).
- 17개 시각 × 47필드가 모두 모이면 그 주기에 `"complete": true`를 붙입니다. 앱은 완성된 주기만
  보여 주므로 기상청 API를 직접 기다릴 일이 없습니다. 주기 시각에서 10시간이 지나도 기상청이 주지 않는
  필드는 포기하고 완성으로 표시합니다(앱이 빠진 자료를 표시).
- **지난 시각은 받지 않습니다:** 앱은 지금과 가장 가까운 예보 시각부터 보여 주므로, 새 주기를 처음 받을 때 한 시간 뒤와 가장 가까운 시각(보통 +6 h)을 `first`로 정해 그 앞 시각은 받지 않습니다. 완성 여부도 `first`부터 셉니다. `--leads`로 지정한 시각은 지났어도 받습니다.
- 최신 주기부터 가장 최근의 완성 주기까지(최대 3개)만 남기고 이전 주기 릴리스는 지웁니다.
- 인증키는 저장소 Secret `KMA_API_KEY`에만 있습니다.
- **하루 용량:** 기상청 API허브 일반회원은 하루 5GB(한국시간 자정 초기화)까지 받을 수 있습니다. 필드는 경량화 API
  (`nwp_nc_vars_down.php`, GRIB2 24비트)로 받아 기존 API(float32)보다 25% 적게 받고(값은 같음, 주기당 약 1.04GB),
  받은 바이트를 `manifest.json`의 `usage`(`{"day": "YYYY-MM-DD"(한국시간), "bytes": …}`)에 적어 **4.9GB를 넘기 전에
  그날 받기를 멈춥니다.** 나머지는 자정 이후 실행이 이어 받습니다. 기상청이 한도 초과 403을 주면 그날은 다 쓴 것으로
  기록합니다. 주기 확인용 1칸 요청만 기존 API를 씁니다. 앱이 직접 기상청에 요청한 양은 이 숫자에 들어가지 않습니다.
- **실행 시작:** GitHub의 `schedule:`은 몇 시간씩 늦거나 빠질 수 있어(2026-10-23에는 한 번도 돌지 않음), 늘 켜져 있는
  PC의 Windows 작업 스케줄러 작업 `KMA relay trigger`가 30분마다(:17, :47) [tools/trigger-relays.ps1](tools/trigger-relays.ps1)로
  두 워크플로를 실행시킵니다(로그인된 GitHub CLI 사용, 로그 `tools/trigger-relays.log`). `schedule:`은 예비로 남겨 둡니다.
  PC가 꺼져 있으면 새 주기가 완성되지 않고, 앱은 반나절 뒤부터 기상청·NOMADS에서 직접 받습니다.

## GFS

[gfs-relay.yml](.github/workflows/gfs-relay.yml)이 30분마다 [gfs_relay.py](gfs_relay.py)를 실행합니다.
앱과 **똑같은 NOMADS grib filter 요청**(100–160°E, 5–55°N, 9개 층 HGT/TMP/UGRD/VGRD/RH, 지표 PRES, PRMSL)을
요청 사이 10초를 두고 보내, 받은 GRIB2를 **그대로** 릴리스 `gfs-<주기>`에 `gfs-<주기>-fLLL-v1.grib2`로 올립니다
(앱 캐시 파일과 같은 이름·내용). 목록은 브랜치 `manifest-gfs`의 `gfs-manifest.json`(형식은 `manifest.json`과 같음)입니다.
17개 시각이 모두 올라오면 완성 표시를 하고, 앱은 완성된 주기만 씁니다.

자료: NOAA/NCEP GFS 0.25° (public domain). NOAA는 이 저장소나 앱을 보증하지 않습니다.

## 파일 형식

`manifest.json`

```json
{"format": 1, "attribution": "출처: 기상청 ...", "updated": "...",
 "cycles": [{"cycle": "2026102218", "tag": "kim-2026102218", "complete": true, "first": 6,
             "leads": {"0": {"file": "kim-2026102218-f000-1a2b3c4d.kimr", "bytes": 1717437, "missing": [], "fields": 47}}}]}
```

`.kimr` (little endian): `"KIMR"`, 형식 버전(1 B), 예약 3 B, 머리글 길이(uint32), UTF-8 JSON 머리글,
필드 자료. 머리글에는 출처 문구, 주기·예보 시각, 격자(241×201, 원 격자 3칸 간격), 필드 목록
(`name`, `level`, 자료 영역 안의 `offset`/`length`)과 받지 못한 필드(`missing`)가 있습니다.
필드마다 앱의 KQ16 형식(값을 0.1 gpm·0.01 K·0.01 m/s·1 Pa 단위 정수로 바꿔 행 방향 차분 후 zlib)입니다.

필드: 지위고도 `hgt`·기온 `T`·바람 `u`/`v`·상대습도 `rh` × 1000/950/925/850/700/500/300/200/100 hPa, 해면기압 `psl`, 지표기압 `ps`.

## 직접 실행

```bash
pip install numpy
KMA_API_KEY=... python kim_relay.py --leads 0 --out out --dry-run
```
