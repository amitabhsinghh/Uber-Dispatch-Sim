import os
import sys
import random
import redis
from dotenv import load_dotenv

load_dotenv()
r = redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"))
count = int(sys.argv[1]) if len(sys.argv) > 1 else 100

# Seed around a city center (e.g., BLR ~ 12.9716, 77.5946)
center_lat, center_lng = 12.9716, 77.5946

for i in range(count):
    # ~ +/- 0.05 degrees jitter (~5-6km)
    lat = center_lat + random.uniform(-0.05, 0.05)
    lng = center_lng + random.uniform(-0.05, 0.05)
    name = f"driver_{i:05d}"
    r.geoadd("drivers:geo", (lng, lat, name))
print(f"seeded {count} drivers into GEO set 'drivers:geo'")
