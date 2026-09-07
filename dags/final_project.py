from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False,
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        s3_bucket='udacity-dend',
        s3_key="log_data/{{ execution_date.strftime('%Y/%m') }}/",
        region='us-west-2',
        json_path='s3://udacity-dend/log_json_path.json',
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        s3_bucket='udacity-dend',
        s3_key='song_data',
        region='us-west-2',
        json_path='auto',
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table='songplays',
        sql_query=SqlQueries.songplay_table_insert,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table='users',
        sql_query=SqlQueries.user_table_insert,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table='songs',
        sql_query=SqlQueries.song_table_insert,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table='artists',
        sql_query=SqlQueries.artist_table_insert,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table='"time"',
        sql_query=SqlQueries.time_table_insert,
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        dq_checks=[
            {'check_sql': 'SELECT COUNT(*) FROM songplays WHERE playid IS NULL', 'expected_result': 0},
            {'check_sql': 'SELECT COUNT(*) FROM users WHERE userid IS NULL', 'expected_result': 0},
            {'check_sql': 'SELECT COUNT(*) FROM songs WHERE songid IS NULL', 'expected_result': 0},
            {'check_sql': 'SELECT COUNT(*) FROM songplays', 'expected_result': 0, 'comparison': '>'},
        ],
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,
                              load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()
