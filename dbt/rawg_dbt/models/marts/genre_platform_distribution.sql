{{ config(
    materialized='table',
    cluster_by=["genre_name", "platform_name"]
) }}

select
    g.genre_name,
    p.platform_name,
    count(distinct gg.game_id) as games_count
from {{ ref('stg_game_genre') }} gg
join {{ ref('stg_genres') }} g
    on gg.genre_id = g.genre_id
join {{ ref('stg_game_platform') }} gp
    on gg.game_id = gp.game_id
join {{ ref('stg_platforms') }} p
    on gp.platform_id = p.platform_id
where gp.platform_id in (4, 7, 187, 186) -- Keep consistency with the rest of the dashboard
group by 1, 2