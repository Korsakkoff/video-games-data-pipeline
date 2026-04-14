import pandas as pd

def build_game_platform_df(raw_games: list[dict]) -> pd.DataFrame:
    game_platform = []

    for game in raw_games:
        game_id = game.get("id")
        for platforms in game.get("platforms", []):
            platform = platforms.get("platform", {})
            platform_id = platform.get("id")
            game_platform.append({
                "game_id": game_id,
                "platform_id": platform_id
            })

    game_platform_df = pd.DataFrame(game_platform)

    if game_platform_df.empty:
        raise ValueError("game_platform_df is empty after transformation")

    game_platform_df = game_platform_df.drop_duplicates(subset=["game_id", "platform_id"])

    return game_platform_df