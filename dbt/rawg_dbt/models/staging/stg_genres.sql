select
    genre_id,
    name as genre_name
from {{ source('rawg_raw', 'genres') }}