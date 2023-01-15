{{ config(materialized='table') }}

with customers as (
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


select
cu.Customer_Names,
pr.Product_Name as Product_Groups,
re.City,
re.Country,
re.Territory,
so.OrderNumber,
so.OrderDate,
so.Channel,
so.Currency_Code,
so.Warehouse_Code,
so.Order_Quantity,
so.Unit_Price,
so.Total_Unit_Cost as Unit_Cost,
so.Line_Total as Total_Sales,
so.Order_Quantity * so.Total_Unit_Cost as Total_Cost,
so.Line_Total - so.Order_Quantity * so.Total_Unit_Cost as Total_Profit,
cast(so.Line_Total - so.Order_Quantity * so.Total_Unit_Cost as numeric)/so.Line_Total as Profit_Margin

from assessment.sales_orders so
inner join assessment.customers cu 
ON so.Customer_Name_Index = cu.Customer_Index
inner join assessment.products pr
ON so.Product_Description_Index = pr.Index
inner join assessment.regions re
ON so.Delivery_Region_Index = re.Index;
