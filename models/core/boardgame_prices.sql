{{ config(materialized='table') }}

with min_price as (
    select distinct
        id,
        name,
        store_url,
        price,
        ROW_NUMBER() OVER(PARTITION BY id ORDER BY price) AS RN
    from {{ ref('stg_games_prices') }}
    where id = 2015
    and date = current_date()
)
select
    min_price.id,
    min_price.name,
    min_price.store_url,
    min_price.price as min_price,
    MAX(stores_prices.price) as max_price,
    (1 - (min_price.price/MAX(stores_prices.price)))*100 AS discount
from min_price
left join  {{ ref('stg_games_prices') }} stores_prices
on min_price.id = stores_prices.id
where RN = 1
GROUP BY id, name, store_url, min_price
ORDER BY discount DESC