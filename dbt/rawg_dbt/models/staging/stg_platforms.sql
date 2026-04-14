select
    platform_id,
    name as platform_name
from {{ source('rawg_raw', 'platforms') }}