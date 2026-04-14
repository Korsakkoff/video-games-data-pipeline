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
group by 1, 2
order by games_count desc, genre_name, platform_name