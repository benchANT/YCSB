CALL refresh_continuous_aggregate('public.daily_machine_usage', 'START_DAY', 'END_DAY', TRUE);

CALL refresh_continuous_aggregate('public.fleet_metrics_15m', 'START_DAY', 'END_DAY', TRUE);

CALL refresh_continuous_aggregate('public.operator_activity_15m', 'START_DAY', 'END_DAY', TRUE);