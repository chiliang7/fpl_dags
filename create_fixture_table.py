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
    'create_fixture_table',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 3, 1, tz='UTC'),
    tags=['fpl_api'],
) as dag:
    dag.doc_md = __doc__

    create_fixtures = PostgresOperator(
        task_id='create_fixture_table',
        postgres_conn_id="postgres",
        database='fpl',
        sql="""
            CREATE TABLE IF NOT EXISTS FIXTURES (
                CODE INT,
                EVENT INT,
                ID INT,
                H_TEAM INT,
                H_TEAM_DIFF INT,
                A_TEAM INT,
                A_TEAM_DIFF INT,
                CREATED_AT TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UPDATED_AT TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY(CODE)
            );

            DROP TRIGGER IF EXISTS set_timestamp_for_fixtures ON FIXTURES;

            create trigger set_timestamp_for_fixtures
            BEFORE UPDATE ON FIXTURES
            FOR EACH ROW
            WHEN (OLD.* IS DISTINCT FROM NEW.*)
            EXECUTE PROCEDURE trigger_set_timestamp();
        """
    )

    create_fixtures

