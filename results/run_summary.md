# final_project DAG — Successful Run Summary

- **DAG run ID:** `manual__2018-11-01T00:00:00+00:00`
- **Logical date:** 2018-11-01 (data interval: 2018-10-31 23:00 → 2018-11-01 00:00)
- **Run type:** manual trigger
- **Final state:** success
- **Total runtime:** ~10:13:49 → 10:14:45 UTC (≈56s)

## Task results

| Task | Operator | State | Duration (s) |
| --- | --- | --- | --- |
| Begin_execution | EmptyOperator | success | 0.0 |
| Stage_events | StageToRedshiftOperator | success | 5.89 |
| Stage_songs | StageToRedshiftOperator | success | 36.34 |
| Load_songplays_fact_table | LoadFactOperator | success | 3.18 |
| Load_user_dim_table | LoadDimensionOperator | success | 5.53 |
| Load_song_dim_table | LoadDimensionOperator | success | 3.997 |
| Load_artist_dim_table | LoadDimensionOperator | success | 5.78 |
| Load_time_dim_table | LoadDimensionOperator | success | 4.80 |
| Run_data_quality_checks | DataQualityOperator | success | 4.78 |
| Stop_execution | EmptyOperator | success | 0.0 |

All 10 tasks completed successfully with the dependency flow required by the rubric:
`Begin_execution → [Stage_events, Stage_songs] → Load_songplays_fact_table → [Load_user_dim_table, Load_song_dim_table, Load_artist_dim_table, Load_time_dim_table] → Run_data_quality_checks → Stop_execution`

## Redshift table row counts (post-run)

| Table | Rows |
| --- | --- |
| staging_songs | 14,896 |
| songplays | 6,820 |
| users | 104 |
| songs | 14,896 |
| artists | 10,025 |
| "time" | 6,820 |

(`staging_events` was subsequently emptied by an unrelated, since-stopped concurrent DAG run with an incorrect execution date — see `task_logs/Stage_events.log` for this run's own successful copy of that data.)

## Data quality checks performed

- `songplays.playid` has no NULLs
- `users.userid` has no NULLs
- `songs.songid` has no NULLs
- `songplays` row count > 0

All checks passed (see `task_logs/Run_data_quality_checks.log`).

## Logs

Per-task logs for this run are included in `task_logs/`.
