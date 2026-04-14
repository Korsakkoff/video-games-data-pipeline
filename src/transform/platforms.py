import pandas as pd

def build_platforms_df(raw_games: list[dict]) -> pd.DataFrame:
    platforms_dict = {}

    for game in raw_games:
        for platforms in game.get("platforms", []):
            platform = platforms.get("platform", {})
            platform_id = platform.get("id")
            platform_name = platform.get("name")
        platforms_dict[platform_id] = platform_name

        platforms_data = [
            {"platform_id": p, "name": n}
            for p, n in platforms_dict.items()
        ]

        platforms_df = pd.DataFrame(platforms_data)

        if platforms_df.empty:
            raise ValueError("platforms_df is empty after transformation")
        
        platforms_df = platforms_df.drop_duplicates(subset=["platform_id"])
    
    return platforms_df