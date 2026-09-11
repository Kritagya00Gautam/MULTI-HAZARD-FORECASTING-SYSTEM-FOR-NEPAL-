import re, json, httpx

URL = "https://dhm.gov.np/hydrology/river-watch"
UA = "nepal-hazard-forecast/0.1 (research; contact: gyanendragautam30915@gmail.com)"

html = httpx.get(URL, headers={"User-Agent": UA}, timeout=60,
                 follow_redirects=True).text
print(f"fetched {len(html):,} bytes")

m = re.search(r"\bcoordinates\s*=\s*(\[.*?\])\s*;", html, re.S)
if not m:
    raise SystemExit("FAILED: 'coordinates' array not found — page structure changed")

stations = json.loads(m.group(1))
assert len(stations) > 300, f"only {len(stations)} stations — suspicious"

with_reading = [s for s in stations if s.get("waterLevel")]
print(f"{len(stations)} stations, {len(with_reading)} with a current reading")

# save a fixture so you can develop the parser offline from here on
with open("dhm_river_sample.json", "w", encoding="utf-8") as f:
    json.dump(stations, f, indent=2, ensure_ascii=False)
print("wrote dhm_river_sample.json")

for s in with_reading[:5]:
    wl = s["waterLevel"]
    print(f"{s['name'].strip():<45} {wl['value']:>7} m  "
          f"warn={s['warning_level']:<6} danger={s['danger_level']:<6} "
          f"{wl['datetime']}")