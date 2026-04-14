select
    game_id,
    genre_id
from {{ source('rawg_raw', 'game_genre') }}