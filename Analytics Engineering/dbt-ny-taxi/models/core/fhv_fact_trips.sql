{{ config(materialized="table") }}

with
    fhv_data as (select *, 'FHV' as service_type from {{ ref("stg_fhv_data") }}),
    dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')
select * from fhv_data inner join dim_zones on fhv_data.pickup_locationid = dim_zones.locationid
