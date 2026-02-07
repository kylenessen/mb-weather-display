import json
from datetime import datetime, timezone

with open("response.json") as f:
    raw = json.load(f)

feeds = raw.get("data", {}).get("groupedFeeds", [])

# Map sensorParameterId to (output key, index in data row)
WANT = {
    3: ("temperature_c", 1),
    339: ("rain_mm", 1),
    5: ("wind_speed_ms", 1),
    6: ("wind_direction_deg", 3),
    370: ("wind_gust_ms", 1),
}

result = {}
latest_time = None

for group in feeds:
    meta_values = group.get("metadata", {}).get("values", [])
    pid_to_idx = {}
    for v in meta_values:
        pid = v["sensorParameterId"]
        if pid in WANT:
            pid_to_idx[pid] = WANT[pid]

    if not pid_to_idx:
        continue

    data = group.get("data", [])
    if not data:
        continue

    last_row = data[-1]
    ts = last_row[0]
    if latest_time is None or ts > latest_time:
        latest_time = ts

    for pid, (key, idx) in pid_to_idx.items():
        if idx < len(last_row):
            result[key] = last_row[idx]

result["timestamp"] = latest_time
result["fetched_at"] = datetime.now(timezone.utc).isoformat()

with open("data.json", "w") as f:
    json.dump(result, f, indent=2)
    f.write("\n")

print(json.dumps(result, indent=2))
