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
    'create_player_tables',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 3, 1, tz='UTC'),
    tags=['fpl_api'],
) as dag:
    dag.doc_md = __doc__

    create_player_histories = PostgresOperator(
        task_id='create_player_histories',
        postgres_conn_id="postgres",
        database='fpl',
        sql="""
            CREATE TABLE IF NOT EXISTS PLAYER_HISTORIES (
                PLAYER_CODE INT,
                FIXTURE_CODE INT,
                EVENT INT,
                ASSISTS INT,
                BONUS INT,
                BPS INT,
                CS INT,
                GC INT,
                GS INT,
                ICT INT,
                TEAM TEXT,
                OPPONENT_TEAM TEXT,
                minutes_played INT,
                penalties_missd INT,
                penalties_saved INT,
                SAVES INT,
                OG INT,
                selected INT,
                POINTS INT,
                transfers_in INT,
                transfers_out INT,
                transfers_balance INT,
                PRICE INT,
                YELLOW_CARDS INT,
                RED_CARDS INT,
                WAS_HOME BOOL,
                CREATED_AT TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UPDATED_AT TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY(PLAYER_CODE, FIXTURE_CODE)
            );

            DROP TRIGGER IF EXISTS set_timestamp ON PLAYER_HISTORIES;

            CREATE TRIGGER set_timestamp
            BEFORE UPDATE ON PLAYER_HISTORIES
            FOR EACH ROW
            WHEN (OLD.* IS DISTINCT FROM NEW.*)
            EXECUTE PROCEDURE trigger_set_timestamp();
        """
    )

    create_player_current_status = PostgresOperator(
        task_id='create_player_current_status',
        postgres_conn_id='postgres',
        database='fpl',
        sql="""
            CREATE TABLE IF NOT EXISTS PLAYER_CURRENT_STATUS (
                code INT PRIMARY KEY,
                id INT,
                first_name TEXT,
                second_name TEXT,
                team_id INT,
                team_code INT,
                chance_of_playing_next_round INT,
                chance_of_playing_this_round INT,
                element_type INT,
                ep_next NUMERIC,
                ep_this NUMERIC,
                form NUMERIC,
                now_cost INT,
                point_per_game NUMERIC,
                total_points INT,
                transfers_in INT,
                transfers_in_event INT,
                transfers_out INT,
                transfers_out_event INT,
                value_form NUMERIC,
                value_season NUMERIC,
                minute_play INT,
                GS INT,
                A INT,
                CS INT,
                GC INT,
                OG INT,
                PS INT,
                PM INT,
                YC INT,
                RC INT,
                S INT,
                BONUS INT,
                BPS INT,
                influence NUMERIC,
                creativity NUMERIC,
                threat NUMERIC,
                ict_index NUMERIC,
                CREATED_AT TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UPDATED_AT TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            DROP TRIGGER IF EXISTS set_timestamp ON PLAYER_CURRENT_STATUS;

            CREATE TRIGGER set_timestamp
            BEFORE UPDATE ON PLAYER_CURRENT_STATUS
            FOR EACH ROW
            WHEN (OLD.* IS DISTINCT FROM NEW.*)
            EXECUTE PROCEDURE trigger_set_timestamp();
        """
    )

    [create_player_histories, create_player_current_status]

