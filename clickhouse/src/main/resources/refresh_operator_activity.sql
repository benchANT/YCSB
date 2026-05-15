WITH 
    toDateTime64('START_DAY', 6) as start_time,
    toDateTime64('END_DAY', 6) as end_time
INSERT INTO TABLE operator_activity_15m (
  time_bucket_15m,
  operator_id,
  working_ping_count,
  idle_ping_count,
  off_ping_count,
  total_ping_count
)   SELECT
            toStartOfInterval(recorded_at, INTERVAL 15 MINUTE) AS time_bucket_15m, operator_id, 
            countIf(engine_state = 'Working') AS working_ping_count,
            countIf(engine_state = 'Idle') AS idle_ping_count,
            countIf(engine_state = 'Off') AS off_ping_count,
            count(*) AS total_ping_count
    FROM telematics_data 
    WHERE recorded_at >= start_time AND recorded_at < end_time
    GROUP BY time_bucket_15m, operator_id
    ORDER BY operator_id, time_bucket_15m;