{{ config(materialized='table') }}

with avg_price as (
    select distinct
    id,
    COUNT(id) AS quantity,
    AVG(price) AS average_price
    from {{ ref('stg_games_prices') }}
    group by id
)
select distinct
   COUNT(bg_data.id) AS total_games,
   CASE
    when average_price BETWEEN 0 AND 15 then '0-15€'
    when average_price BETWEEN 15 AND 30 then '16-30€'
    when average_price BETWEEN 31 AND 45 then '31-45€'
    when average_price BETWEEN 46 AND 60 then '46-60€'
    else '+60€'
    end as price_range
from avg_price
left join {{ ref('stg_boardgame_data') }} as bg_data
on avg_price.id = bg_data.id
group by price_range
HAVING total_games > 0
ORDER BY 1 DESC