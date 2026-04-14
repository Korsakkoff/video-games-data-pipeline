select
    date(extract(year from released_date), 1, 1) as release_year,
    count(*) as games_count
from {{ ref('stg_games') }}
where released_date is not null
group by 1
order by 1