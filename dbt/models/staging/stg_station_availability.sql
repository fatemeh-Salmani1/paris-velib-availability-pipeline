with source_data as (

    select *
    from {{ source('velib_raw', 'station_availability') }}

),

typed_and_enriched as (

    select
        timestamp_micros(div(ts_utc, 1000)) as observed_at,
        timestamp_micros(div(tbin_utc, 1000)) as time_bin,

        date(timestamp_micros(div(tbin_utc, 1000))) as observation_date,
        extract(hour from timestamp_micros(div(tbin_utc, 1000))) as observation_hour,
        format_timestamp(
            '%A',
            timestamp_micros(div(tbin_utc, 1000))
        ) as weekday_name,

        station_id,
        name as station_name,
        lat as latitude,
        lon as longitude,
        status,

        bikes as available_bikes,
        capacity,
        mechanical as available_mechanical_bikes,
        ebike as available_ebikes,
        capacity - bikes as available_docks,

        temp_C as temperature_c,
        precip_mm as precipitation_mm,
        wind_mps as wind_speed_mps,

        status = 'CLOSED' as is_closed,
        status = 'OK' and bikes = 0 as is_empty,
        status = 'OK' and capacity > 0 and bikes >= capacity as is_full,

        mechanical + ebike != bikes as has_inconsistent_bike_total,
        bikes > capacity as has_bikes_over_capacity,

        (
            bikes < 0
            or capacity < 0
            or mechanical < 0
            or ebike < 0
        ) as has_negative_count

    from source_data

)

select *
from typed_and_enriched