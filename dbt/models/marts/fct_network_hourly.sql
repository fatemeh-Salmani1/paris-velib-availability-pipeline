with station_observations as (

    select *
    from {{ ref('stg_station_availability') }}

),

time_bin_metrics as (

    select
        time_bin,

        count(distinct station_id) as observed_stations,
        countif(status = 'OK') as operational_stations,
        countif(is_closed) as closed_stations,
        countif(is_empty) as empty_stations,
        countif(is_full) as full_stations,

        sum(
            if(
                status = 'OK'
                and not has_negative_count
                and not has_bikes_over_capacity,
                available_bikes,
                0
            )
        ) as network_available_bikes,

        sum(
            if(
                status = 'OK'
                and not has_negative_count
                and not has_bikes_over_capacity,
                available_docks,
                0
            )
        ) as network_available_docks,

        avg(temperature_c) as temperature_c,
        avg(precipitation_mm) as precipitation_mm,
        avg(wind_speed_mps) as wind_speed_mps

    from station_observations
    group by time_bin

),

hourly_metrics as (

    select
        timestamp_trunc(time_bin, hour) as hour_start_utc,

        datetime(
            timestamp_trunc(time_bin, hour),
            'Europe/Paris'
        ) as hour_start_local,

        date(time_bin, 'Europe/Paris') as observation_date,

        extract(
            hour from datetime(time_bin, 'Europe/Paris')
        ) as observation_hour,

        format_datetime(
            '%A',
            datetime(time_bin, 'Europe/Paris')
        ) as weekday_name,

        count(*) as time_bins_observed,

        avg(observed_stations) as avg_observed_stations,
        avg(operational_stations) as avg_operational_stations,
        avg(closed_stations) as avg_closed_stations,
        avg(empty_stations) as avg_empty_stations,
        avg(full_stations) as avg_full_stations,

        avg(network_available_bikes) as avg_network_available_bikes,
        avg(network_available_docks) as avg_network_available_docks,

        safe_divide(
            sum(empty_stations),
            sum(operational_stations)
        ) as empty_station_rate,

        safe_divide(
            sum(full_stations),
            sum(operational_stations)
        ) as full_station_rate,

        safe_divide(
            sum(empty_stations + full_stations),
            sum(operational_stations)
        ) as availability_issue_rate,

        avg(temperature_c) as avg_temperature_c,
        avg(precipitation_mm) as avg_precipitation_mm,
        avg(wind_speed_mps) as avg_wind_speed_mps

    from time_bin_metrics
    group by
        hour_start_utc,
        hour_start_local,
        observation_date,
        observation_hour,
        weekday_name

)

select
    *,

    round(100 * empty_station_rate, 2) as empty_station_rate_pct,
    round(100 * full_station_rate, 2) as full_station_rate_pct,
    round(100 * availability_issue_rate, 2)
        as availability_issue_rate_pct

from hourly_metrics