{{ config(materialized='table') }}

select
    COUNT(DISTINCT id) AS total_games,
    COUNT(DISTINCT id) * 100.0 / SUM(COUNT(DISTINCT id)) OVER() as percentage_total_games,
    year_published
from {{ ref('stg_boardgame_data') }}
where date = current_date()
and year_published is not null
and year_published <= 2022
group by year_published
order by 1 desc