with time_bin_counts as (

    select
        (
            select count(distinct time_bin)
            from {{ ref('stg_station_availability') }}
        ) as staging_time_bins,

        (
            select sum(time_bins_observed)
            from {{ ref('fct_network_hourly') }}
        ) as mart_time_bins

)

select
    staging_time_bins,
    mart_time_bins
from time_bin_counts
where staging_time_bins != mart_time_bins