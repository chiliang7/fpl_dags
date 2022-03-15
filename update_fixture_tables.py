#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pendulum
import requests
import json
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

def sanitized_input(x):
    return 0 if x == None else x

with DAG(
    'update_fixture_tables',
    schedule_interval="0 7 * *  thu,fri",
    start_date=pendulum.datetime(2022, 3, 1, tz='UTC'),
    tags=['fpl_api'],
) as dag:
    dag.doc_md = __doc__

    def extract_fixtures(**kwargs):
        rows = ""
        ti = kwargs['ti']

        for event in range(1, 39):
            response_API = requests.get("https://fantasy.premierleague.com/api/fixtures?event={}".format(event))
            fixtures = json.loads(response_API.text)

            for fixture in fixtures:
                row = """
                (
                    {code},
                    {event},
                    {id},
                    {h_team},
                    {h_team_diff},
                    {a_team},
                    {a_team_diff},
                    {h_score},
                    {a_score}
                ),""".format(
                    code=fixture['code'],
                    event=fixture['event'], 
                    id=fixture['id'], 
                    h_team=fixture['team_h'], 
                    h_team_diff=fixture['team_h_difficulty'],
                    a_team=fixture['team_a'],
                    a_team_diff=fixture['team_a_difficulty'],
                    h_score=sanitized_input(fixture['team_h_score']),
                    a_score=sanitized_input(fixture['team_a_score'])
                )                

                rows += row

        sql_query = """
            INSERT INTO FIXTURES(code, event, id, h_team, h_team_diff, a_team, a_team_diff, h_score, a_score) 
            VALUES 
            {}
            ON CONFLICT (CODE) DO UPDATE SET
            (code, event, id, h_team, h_team_diff, a_team, a_team_diff, h_score, a_score) = (
                Excluded.code,
                Excluded.event,
                Excluded.id,
                Excluded.h_team,
                Excluded.h_team_diff,
                Excluded.a_team,
                Excluded.a_team_diff,
                Excluded.h_score,
                Excluded.a_score
            );
        """.format(rows[:-1])

        ti.xcom_push('fixture_data', sql_query)
            
    extrack_task = PythonOperator(
        task_id='extract_fixtures',
        python_callable=extract_fixtures
    )

    load_task = PostgresOperator(
        task_id='upsert_fixture',
        postgres_conn_id='postgres',
        database='fpl',
        sql="{{ ti.xcom_pull(task_ids='extract_fixtures', key='fixture_data') }}"
    )
    
    extrack_task >> load_task

