{{ config(materialized='table') }}

with customer as (
    select *
    from {{ source('staging','customers') }}
), 

with products as (
    select *
    from {{ source('staging','products') }}
), 

with regions as (
    select *
    from {{ source('staging','regions') }}
), 

with sales_orders as (
    select *
    from {{ source('staging','sales_orders') }}
)



