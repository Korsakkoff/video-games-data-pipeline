#!/usr/bin/env python
# coding: utf-8

import json
from pathlib import Path

import json
from pathlib import Path


def read_raw_games(raw_dir: Path) -> list[dict]:
    json_files = sorted(raw_dir.glob("*.json"))

    if not json_files:
        raise FileNotFoundError(f"No JSON files found in {raw_dir}")

    all_games: list[dict] = []

    for file_path in json_files:
        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        results = data.get("results", [])

        if not isinstance(results, list):
            raise ValueError(f"'results' is not a list in file: {file_path.name}")

        all_games.extend(results)

    if not all_games:
        raise ValueError("No games were loaded from raw JSON files")

    return all_games