with
    tripdata as (
        select *, row_number() over (partition by pickup_datetime) as rn
        from {{ source("staging", "fhv_data") }}
    )
select 

    dispatching_base_num,
    COALESCE(SR_FLag, 2) AS SR_FLag,
    -- CAST(COALESCE(pulocationid, 1) AS INT) AS pickup_locationid,
    -- CAST(COALESCE(dolocationid, 1) AS INT) AS dropoff_locationid,
    pulocationid as pickup_locationid, 
    dolocationid as dropoff_locationid,
    TIMESTAMP_MICROS(CAST(pickup_datetime / 1000 AS INT64)) AS  pickup_datetime,
    TIMESTAMP_MICROS(CAST(dropoff_datetime / 1000 AS INT64)) AS  dropoff_datetime

from tripdata