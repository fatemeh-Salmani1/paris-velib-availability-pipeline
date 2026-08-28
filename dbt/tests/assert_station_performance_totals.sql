with row_counts as (

    select
        (
            select count(*)
            from {{ ref('stg_station_availability') }}
        ) as staging_rows,

        (
            select sum(total_observations)
            from {{ ref('fct_station_performance') }}
        ) as mart_rows

)

select
    staging_rows,
    mart_rows
from row_counts
where staging_rows != mart_rows