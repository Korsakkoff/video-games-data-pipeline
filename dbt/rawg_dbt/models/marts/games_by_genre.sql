{{ config(
    materialized='table',
    cluster_by=["genre_name"]
) }}

select
    g.genre_name,
    count(distinct gg.game_id) as games_count
from {{ ref('stg_game_genre') }} gg
join {{ ref('stg_genres') }} g
    on gg.genre_id = g.genre_id
group by 1