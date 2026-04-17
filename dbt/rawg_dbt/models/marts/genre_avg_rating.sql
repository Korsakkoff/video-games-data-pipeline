{{ config(
    materialized='table',
    cluster_by=["genre_name"]
) }}

select
    g.genre_name,
    count(distinct gg.game_id) as games_count,
    round(avg(sg.rating), 2) as avg_rating
from {{ ref('stg_game_genre') }} gg
join {{ ref('stg_genres') }} g
    on gg.genre_id = g.genre_id
join {{ ref('stg_games') }} sg
    on gg.game_id = sg.game_id
where sg.rating is not null
group by 1