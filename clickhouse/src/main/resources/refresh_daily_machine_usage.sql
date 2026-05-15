WITH
    now() AS current_ts,
    24*60*60 AS bucket_length_time,
    toDateTime64('START_DAY', 6) as start_time,
    toDateTime64('END_DAY', 6) as end_time,
    engine_state_changes AS (
        SELECT * 
        FROM (
            SELECT
                vin,
                recorded_at,
                engine_state,
                lag(engine_state) OVER (PARTITION BY vin ORDER BY recorded_at) AS prev_state
                FROM telematics_data
                WHERE recorded_at >= start_time AND recorded_at <= end_time
                -- AND vin = 'VIN-0'
            )
        WHERE engine_state != prev_state OR prev_state IS NULL
    ),
    engine_state_flattened AS (
        SELECT 
            vin,
            engine_state,
            recorded_at AS start_ts ,
            lead(toNullable(recorded_at)) OVER (PARTITION BY vin ORDER BY recorded_at) AS end_ts,
            arrayJoin(
                range(
                    toUInt32(toUnixTimestamp(start_ts) / bucket_length_time),
                    toUInt32(toUnixTimestamp(coalesce(end_ts, end_time)) / bucket_length_time) + 1
                )
            ) AS bucket_id,
            toDateTime(bucket_id * bucket_length_time) AS bucket_start,
            (bucket_start + Interval '1 DAY') as bucket_end 
        FROM engine_state_changes
    ),
    engine_state_durations AS (
        SELECT
            vin,
            engine_state,
            greatest(0,
            dateDiff('second',
                    greatest(start_ts, bucket_start),
                    least(end_ts, bucket_end)
            )) AS duration,
            start_ts,
            end_ts,
            bucket_start,
            bucket_end
        FROM engine_state_flattened
        WHERE start_ts < end_time AND bucket_start < end_time
    ),
    duration_per_state_and_bucket AS (
        SELECT
            vin, 
            bucket_start as time_bucket_1d,
            sumIf(duration, engine_state = 'Working') AS working_seconds,
            sumIf(duration, engine_state = 'Idle') AS idle_seconds,
            sumIf(duration, engine_state = 'Off') AS off_seconds
        FROM engine_state_durations
        GROUP BY vin, bucket_start
        ORDER BY vin, time_bucket_1d
    )
INSERT INTO daily_machine_usage
SELECT
    time_bucket_1d::Date as usage_date , vin, working_seconds, idle_seconds, off_seconds
 FROM duration_per_state_and_bucket