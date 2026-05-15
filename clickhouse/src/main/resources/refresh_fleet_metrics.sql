--- this is for fleet metrics
WITH 
    900 AS bucket_length_time,
    toDateTime64('START_DAY', 6) as start_time,
    toDateTime64('END_DAY', 6) as end_time,
    engine_state_changes AS (SELECT * 
        FROM (
            SELECT
                vin,
                recorded_at,
                engine_state,
                lag(engine_state) OVER (PARTITION BY vin ORDER BY recorded_at) AS prev_state
                FROM telematics_data
                WHERE recorded_at >= start_time AND recorded_at <= end_time
            )
        WHERE (engine_state != prev_state OR prev_state IS NULL)
         --   AND vin LIKE 'VIN-100'
        ORDER BY recorded_at ASC
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
            (bucket_start + Interval '15 MINUTE') as bucket_end 
        FROM engine_state_changes
    ),
    engine_state_durations AS (
        SELECT
            vin,
            engine_state,
            greatest(start_ts, bucket_start) as cleared_start,
            least(end_ts, bucket_end) as cleared_end,
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
            bucket_start as time_bucket_15m,
            sumIf(duration, engine_state = 'Working') AS working_seconds,
            sumIf(duration, engine_state = 'Idle') AS idle_seconds,
            sumIf(duration, engine_state = 'Off') AS off_seconds
        FROM engine_state_durations
        GROUP BY vin, bucket_start
        ORDER BY vin, time_bucket_15m
    ),
    other_states AS (
        SELECT
            toStartOfInterval(recorded_at, INTERVAL 15 MINUTE) AS time_bucket_15m,
            vin,
            argMin(fuel_level_percent, recorded_at) AS starting_fuel,
            argMax(fuel_level_percent, recorded_at) AS ending_fuel,
            max(engine_rpm) AS max_engine_rpm,
            min(engine_rpm) AS min_engine_rpm,
            max(coolant_temperature) AS max_coolant_temperature,
            min(coolant_temperature) AS min_coolant_temperature,
            max(hydraulic_oil_temperature) AS max_hydraulic_oil_temperature,
            min(hydraulic_oil_temperature) AS min_hydraulic_oil_temperature,
            argMax(latitude, recorded_at) AS last_latitude,
            argMax(longitude, recorded_at) AS last_longitude,
            argMin(latitude, recorded_at) AS start_latitude,
            argMin(longitude, recorded_at) AS start_longitude
        FROM telematics_data 
        WHERE recorded_at >= start_time AND recorded_at < end_time
        GROUP BY time_bucket_15m, vin
    ),
    full_result_set AS (
        SELECT
            time_bucket_15m, other_states.vin, working_seconds, idle_seconds, off_seconds, starting_fuel,
            ending_fuel, max_engine_rpm, min_engine_rpm, max_coolant_temperature, 
            min_coolant_temperature, max_hydraulic_oil_temperature, min_hydraulic_oil_temperature, 
            last_latitude, last_longitude, start_latitude, start_longitude
        FROM other_states LEFT JOIN duration_per_state_and_bucket
            ON
            other_states.time_bucket_15m = duration_per_state_and_bucket.time_bucket_15m
            AND
            other_states.vin = duration_per_state_and_bucket.vin 
        ORDER BY vin, time_bucket_15m
    )
INSERT INTO TABLE fleet_metrics_15m (
  time_bucket_15m ,
  vin,
  working_seconds,
  idle_seconds,
  off_seconds,
  starting_fuel,
  ending_fuel,
  max_engine_rpm,
  min_engine_rpm,
  max_coolant_temperature,
  min_coolant_temperature,
  max_hydraulic_oil_temperature,
  min_hydraulic_oil_temperature,
  last_latitude,
  last_longitude,
  start_latitude,
  start_longitude
)   SELECT
            time_bucket_15m, vin, working_seconds, idle_seconds, off_seconds, starting_fuel,
            ending_fuel, max_engine_rpm, min_engine_rpm, max_coolant_temperature, 
            min_coolant_temperature, max_hydraulic_oil_temperature, min_hydraulic_oil_temperature, 
            last_latitude, last_longitude, start_latitude, start_longitude
    FROM full_result_set