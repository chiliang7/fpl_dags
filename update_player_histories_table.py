#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pendulum
import requests
import json
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import psycopg2 

with DAG(
    'update_player_histories',
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 3, 7, tz='UTC'),
    tags=['fpl_api']
) as dag:
    dag.doc_md = __doc__

    def extract_player(**kwargs):
        ti              = kwargs['ti']
        uniq_player_ids = None
        player_id_dict  = {}
        team_name_dict  = {}
        fixture_dict    = {}
        rows = ""
        user_name = None
        password  = None

        with psycopg2.connect("dbname=fpl user={} password={} host=localhost".format(user_name, password)) as conn:
            with conn.cursor() as cur:

                cur.execute("""
                    SELECT ID, CODE, TEAM_ID
                    FROM PLAYER_CURRENT_STATUS
                    ORDER BY ID;
                """)

                # for code, id in cur.fetchall():
                player_id_dict = dict([(tup[0], (tup[1], tup[2])) for tup in cur.fetchall()])

                cur.execute("""
                    SELECT ID, SHORT_NAME
                    FROM TEAMS;
                """)

                team_name_dict = dict(cur.fetchall())

                cur.execute("""
                    SELECT ID, CODE, EVENT
                    FROM FIXTURES;
                """)

                fixture_dict = dict([(tup[0], (tup[1], tup[2])) for tup in cur.fetchall()])
        
        for player_id in player_id_dict.keys():
            response_API = requests.get("https://fantasy.premierleague.com/api/element-summary/{}/".format(player_id))
            data         = response_API.text
            player_data  = json.loads(data)

            for history in player_data['history']:
                row = """
                    (
                        {player_code},
                        {fixture_code},
                        {event},
                        {assists},
                        {bonus},
                        {bps},
                        {cs},
                        {gc},
                        {gs},
                        {ict},
                        '{team}',
                        '{opponent_team}', 
                        {minutes_played},
                        {penalties_missed},
                        {penalties_saved},
                        {saves},
                        {og},
                        {selected},
                        {points},
                        {transfers_in},
                        {transfers_out},
                        {transfers_balance},
                        {price},
                        {yellow_cards},
                        {red_cards},
                        {was_home}
                    ),""".format(
                        player_code=player_id_dict[history['element']][0],
                        fixture_code=fixture_dict[history['fixture']][0],
                        event=fixture_dict[history['fixture']][1],
                        assists=history['assists'],
                        bonus=history['bonus'],
                        bps=history['bps'],
                        cs=history['clean_sheets'],
                        gc=history['goals_conceded'],
                        gs=history['goals_scored'],
                        ict=float(history['ict_index']),
                        team=team_name_dict[player_id_dict[history['element']][1]],
                        opponent_team=team_name_dict[history['opponent_team']],
                        minutes_played=history['minutes'],
                        penalties_missed=history['penalties_missed'],
                        penalties_saved=history['penalties_saved'],
                        saves=history['saves'],
                        og=history['own_goals'],
                        selected=history['selected'],
                        points=history['total_points'],
                        transfers_in=history['transfers_in'],
                        transfers_out=history['transfers_out'],
                        transfers_balance=history['transfers_balance'],
                        price=history['value'],
                        yellow_cards=history['yellow_cards'],
                        red_cards=history['red_cards'],
                        was_home=history['was_home']                        
                    )
                rows += row

        sql_query = """
            INSERT INTO PLAYER_HISTORIES VALUES
            {}
            ON CONFLICT (PLAYER_CODE, FIXTURE_CODE) DO UPDATE SET
            (
                PLAYER_CODE,
                FIXTURE_CODE,
                EVENT,
                ASSISTS,
                BONUS,
                BPS,
                CS,
                GC,
                GS,
                ICT,
                TEAM,
                OPPONENT_TEAM,
                minutes_played,
                penalties_missd,
                penalties_saved,
                SAVES,
                OG,
                selected,
                POINTS,
                transfers_in,
                transfers_out,
                transfers_balance,
                PRICE,
                YELLOW_CARDS,
                RED_CARDS,
                WAS_HOME
            ) = (
                Excluded.PLAYER_CODE,
                Excluded.FIXTURE_CODE,
                Excluded.EVENT,
                Excluded.ASSISTS,
                Excluded.BONUS,
                Excluded.BPS,
                Excluded.CS,
                Excluded.GC,
                Excluded.GS,
                Excluded.ICT,
                Excluded.TEAM,
                Excluded.OPPONENT_TEAM,
                Excluded.minutes_played,
                Excluded.penalties_missd,
                Excluded.penalties_saved,
                Excluded.SAVES,
                Excluded.OG,
                Excluded.selected,
                Excluded.POINTS,
                Excluded.transfers_in,
                Excluded.transfers_out,
                Excluded.transfers_balance,
                Excluded.PRICE,
                Excluded.YELLOW_CARDS,
                Excluded.RED_CARDS,
                Excluded.WAS_HOME
            );
        """.format(rows[:-1])

        ti.xcom_push('player_histories', sql_query)

    load_task = PostgresOperator(
        task_id='upsert_player_histories',
        postgres_conn_id='postgres',
        database='fpl',
        sql="{{ ti.xcom_pull(task_ids='extract_player', key='player_histories') }}"
    )

    extract_task = PythonOperator(
        task_id='extract_player',
        python_callable=extract_player
    )
        
    extract_task >> load_task

    


