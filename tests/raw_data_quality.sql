WITH source AS (
    SELECT *
    FROM `paris-velib-de-fs-2026.velib_raw.station_availability`
),

duplicate_summary AS (
    SELECT
        COALESCE(SUM(record_count - 1), 0) AS duplicate_rows
    FROM (
        SELECT
            station_id,
            tbin_utc,
            COUNT(*) AS record_count
        FROM source
        GROUP BY station_id, tbin_utc
        HAVING COUNT(*) > 1
    )
),

quality_summary AS (
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT station_id) AS unique_stations,
        COUNT(DISTINCT tbin_utc) AS unique_time_bins,

        COUNTIF(
            station_id IS NULL
            OR ts_utc IS NULL
            OR tbin_utc IS NULL
        ) AS missing_key_values,

        COUNTIF(
            bikes IS NULL
            OR capacity IS NULL
            OR mechanical IS NULL
            OR ebike IS NULL
            OR status IS NULL
            OR lat IS NULL
            OR lon IS NULL
            OR name IS NULL
            OR temp_C IS NULL
            OR precip_mm IS NULL
            OR wind_mps IS NULL
        ) AS rows_with_missing_values,

        COUNTIF(
            bikes < 0
            OR capacity < 0
            OR mechanical < 0
            OR ebike < 0
        ) AS rows_with_negative_counts,

        COUNTIF(bikes > capacity)
            AS bikes_exceeding_capacity,

        COUNTIF(mechanical + ebike != bikes)
            AS inconsistent_bike_totals,

        COUNTIF(status = 'CLOSED')
            AS closed_station_observations,

        COUNTIF(status NOT IN ('OK', 'CLOSED'))
            AS unexpected_status_values

    FROM source
)

SELECT
    quality_summary.*,
    duplicate_summary.duplicate_rows
FROM quality_summary
CROSS JOIN duplicate_summary;