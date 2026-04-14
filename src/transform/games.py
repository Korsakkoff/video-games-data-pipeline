import pandas as pd

def build_games_df(raw_games: list[dict]) -> pd.DataFrame:
    games_data: list[dict] = []

    for game in raw_games:
        games_data.append(
            {
                "game_id": game.get("id"),
                "name": game.get("name"),
                "released": game.get("released"),
                "rating": game.get("rating"),
                "ratings_count": game.get("ratings_count"),
            }
        )

    games_df = pd.DataFrame(games_data)

    if games_df.empty:
        raise ValueError("games_df is empty after transformation")

    games_df = games_df.drop_duplicates(subset=["game_id"])

    return games_df