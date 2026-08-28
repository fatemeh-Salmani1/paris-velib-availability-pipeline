with station_observations as (

    select *
    from {{ ref('stg_station_availability') }}

),

station_aggregates as (

    select
        station_id,

        array_agg(
            station_name
            ignore nulls
            order by observed_at desc
            limit 1
        )[safe_offset(0)] as station_name,

        array_agg(
            latitude
            ignore nulls
            order by observed_at desc
            limit 1
        )[safe_offset(0)] as latitude,

        array_agg(
            longitude
            ignore nulls
            order by observed_at desc
            limit 1
        )[safe_offset(0)] as longitude,

        min(observed_at) as first_observation_at,
        max(observed_at) as last_observation_at,

        count(*) as total_observations,
        countif(status = 'OK') as operational_observations,
        countif(is_closed) as closed_observations,
        countif(is_empty) as empty_observations,
        countif(is_full) as full_observations,

        avg(
            if(
                status = 'OK'
                and not has_negative_count
                and not has_bikes_over_capacity,
                available_bikes,
                null
            )
        ) as avg_available_bikes,

        avg(
            if(
                status = 'OK'
                and not has_negative_count
                and not has_bikes_over_capacity,
                available_docks,
                null
            )
        ) as avg_available_docks,

        avg(
            if(
                status = 'OK'
                and not has_negative_count,
                capacity,
                null
            )
        ) as avg_capacity,

        countif(
            has_inconsistent_bike_total
            or has_bikes_over_capacity
            or has_negative_count
        ) as quality_issue_observations

    from station_observations
    group by station_id

),

station_rates as (

    select
        *,

        safe_divide(
            empty_observations,
            operational_observations
        ) as empty_rate,

        safe_divide(
            full_observations,
            operational_observations
        ) as full_rate,

        safe_divide(
            closed_observations,
            total_observations
        ) as closed_rate,

        safe_divide(
            empty_observations + full_observations,
            operational_observations
        ) as availability_issue_rate,

        safe_divide(
            quality_issue_observations,
            total_observations
        ) as quality_issue_rate

    from station_aggregates

),

scored_stations as (

    select
        *,

        round(100 * empty_rate, 2) as empty_rate_pct,
        round(100 * full_rate, 2) as full_rate_pct,
        round(100 * closed_rate, 2) as closed_rate_pct,
        round(100 * availability_issue_rate, 2)
            as availability_issue_rate_pct,
        round(100 * quality_issue_rate, 2)
            as quality_issue_rate_pct,

        round(
            100 * availability_issue_rate,
            2
        ) as rebalancing_priority_score

    from station_rates

)

select
    *,

    case
        when operational_observations = 0 then 'NO_OPERATIONAL_DATA'
        when rebalancing_priority_score >= 10 then 'HIGH'
        when rebalancing_priority_score >= 5 then 'MEDIUM'
        else 'LOW'
    end as rebalancing_priority_level

from scored_stations