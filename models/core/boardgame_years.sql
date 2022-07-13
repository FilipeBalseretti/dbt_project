{{ config(materialized='table') }}

select 
    COUNT(id) AS total_games,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage_total_games,
    year_published
from {{ ref('stg_boardgame_data') }}
where date = current_date()
and year_published is not null
group by year_published
order by 1 desc