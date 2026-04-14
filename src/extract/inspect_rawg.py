import json

with open("data/raw/rawg_games_test.json", "r", encoding="utf-8") as f:
    data = json.load(f)

results = data.get("results", [])

print(f"Total results in file: {len(results)}")

first = results[0]

print("\nTop-level fields:")
for key in first.keys():
    print(key)

print("\nSample game:")
print({
    "id": first.get("id"),
    "name": first.get("name"),
    "released": first.get("released"),
    "rating": first.get("rating"),
    "ratings_count": first.get("ratings_count"),
})

print("\nGenres structure:")
print(first.get("genres"))

print("\nPlatforms structure:")
print(first.get("platforms"))