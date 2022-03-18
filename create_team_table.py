#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json

import pendulum
import requests
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

with DAG(
    'create_team_table',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 3, 1, tz='UTC'),
    tags=['fpl_api'],
) as dag:
    dag.doc_md = __doc__

    create_team_info_table = PostgresOperator(
        task_id='create_team_info_table',
        postgres_conn_id="postgres",
        database='fpl',
        sql="""
            CREATE TABLE IF NOT EXISTS TEAMS (
                ID integer PRIMARY KEY,
                NAME text,
                SHORT_NAME text,
                STRENGTH integer,
                STRENGTH_OVERALL_AWAY integer,
                STRENGTH_OVERALL_HOME integer,
                CREATED_AT TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UPDATED_AT TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            DROP TRIGGER IF EXISTS set_timestamp ON TEAM;

            CREATE TRIGGER set_timestamp
            BEFORE UPDATE ON TEAMS
            FOR EACH ROW
            WHEN (OLD.* IS DISTINCT FROM NEW.*)
            EXECUTE PROCEDURE trigger_set_timestamp();
        """
    )
    
    def extract_team_info(**kwargs):
        ti           = kwargs['ti']
        response_API = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/")
        data         = response_API.text
        basic_data   = json.loads(data)
        rows         = ""

        for team in basic_data['teams']:
            row = "('{}', '{}', '{}', '{}', '{}', '{}'),".format(
                team['id'], 
                team['name'], 
                team['short_name'], 
                team['strength'], 
                team['strength_overall_away'],
                team['strength_overall_home']
            )

            rows += row

        sql_query = """
            INSERT INTO TEAMS VALUES 
            {}
            ON CONFLICT (id) DO UPDATE SET
                (
                    name, 
                    short_name, 
                    strength, 
                    strength_overall_away, 
                    strength_overall_home
                ) = (
                    Excluded.name, 
                    Excluded.short_name,
                    Excluded.strength, 
                    Excluded.strength_overall_away, 
                    Excluded.strength_overall_home
                );
        """.format(rows[:-1])

        ti.xcom_push('team_data', sql_query)

    load_task = PostgresOperator(
        task_id='upsert_team_info',
        postgres_conn_id='postgres',
        database='fpl',
        sql="{{ ti.xcom_pull(task_ids='extract_team_info', key='team_data')}}"
    )

    extract_task = PythonOperator(task_id='extract_team_info', python_callable=extract_team_info)
    
    create_team_info_table >> extract_task >> load_task

