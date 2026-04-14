import pandas as pd

def build_genres_df(raw_games: list[dict]) -> pd.DataFrame:
    genres_dict = {}

    for game in raw_games:
        for genre in game.get("genres", []):
            genre_id = genre.get("id")
            genre_name = genre.get("name")
        genres_dict[genre_id] = genre_name

        genres_data = [
            {"genre_id": g, "name": n}
            for g, n in genres_dict.items()
        ]

        genres_df = pd.DataFrame(genres_data)

        if genres_df.empty:
            raise ValueError("genres_df is empty after transformation")
        
        genres_df = genres_df.drop_duplicates(subset=["genre_id"])
    
    return genres_df