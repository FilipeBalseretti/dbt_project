{{ config(materialized='view') }}

select * from {{ source('staging', 'boardgames') }}
limit 100