import pandas as pd

def build_game_genre_df(raw_games: list[dict]) -> pd.DataFrame:
    game_genre = []

    for game in raw_games:
        game_id = game.get("id")
        for genre in game.get("genres", []):
            genre_id = genre.get("id")
            game_genre.append({
                "game_id": game_id,
                "genre_id": genre_id
            })

    game_genre_df = pd.DataFrame(game_genre)

    if game_genre_df.empty:
        raise ValueError("game_genre_df is empty after transformation")

    game_genre_df = game_genre_df.drop_duplicates(subset=["game_id", "genre_id"])

    return game_genre_df