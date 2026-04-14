select
    game_id,
    platform_id
from {{ source('rawg_raw', 'game_platform') }}