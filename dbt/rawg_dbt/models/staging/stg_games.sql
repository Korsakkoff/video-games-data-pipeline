select
    game_id,
    name,
    safe_cast(released as date) as released_date,
    rating,
    ratings_count,
    playtime,
    added,
    metacritic
from {{ source('rawg_raw', 'games') }}