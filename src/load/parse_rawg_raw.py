from pathlib import Path
import json

raw_dir = Path("data/raw")
json_files = sorted(raw_dir.glob("*.json"))

for json_file in json_files:
    print(f"Processing file: {json_file}")

all_games = []
files_processed = 0