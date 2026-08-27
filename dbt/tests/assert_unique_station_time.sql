select
    station_id,
    time_bin,
    count(*) as record_count
from {{ ref('stg_station_availability') }}
group by
    station_id,
    time_bin
having count(*) > 1