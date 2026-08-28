select
    station_id,
    empty_rate,
    full_rate,
    closed_rate,
    availability_issue_rate,
    quality_issue_rate
from {{ ref('fct_station_performance') }}
where
    empty_rate not between 0 and 1
    or full_rate not between 0 and 1
    or closed_rate not between 0 and 1
    or availability_issue_rate not between 0 and 1
    or quality_issue_rate not between 0 and 1