{{ config(materialized='table') }}

select * from {{ ref('sales_data_seed') }}
