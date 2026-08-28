select
    hour_start_utc,
    empty_station_rate,
    full_station_rate,
    availability_issue_rate
from {{ ref('fct_network_hourly') }}
where
    empty_station_rate not between 0 and 1
    or full_station_rate not between 0 and 1
    or availability_issue_rate not between 0 and 1