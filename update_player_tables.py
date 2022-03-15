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
    return 'null' if x == None else x

with DAG(
    'update_player_tables',
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 3, 1, tz='UTC'),
    tags=['fpl_api'],
) as dag:
    dag.doc_md = __doc__

    def extract_player_current_status(**kwargs):
        response_API = requests.get("https://fantasy.premierleague.com/api/bootstrap-static/")
        data         = response_API.text
        basic_data   = json.loads(data)
        rows         = ""
        ti           = kwargs['ti']

        for player in basic_data['elements']:
            row = """
            (
                {code},
                {id},
                '{first_name}',
                '{second_name}',
                {team_id},
                {team_code},
                {chance_of_playing_next_round},
                {chance_of_playing_this_round},
                {element_type},
                {ep_next},
                {ep_this},
                {form},
                {now_cost},
                {point_per_game},
                {total_points},
                {transfers_in},
                {transfers_in_event},
                {transfers_out},
                {transfers_out_event},
                {value_form},
                {value_season},
                {minute_play},
                {GS},
                {A},
                {CS},
                {GC},
                {OG},
                {PS},
                {PM},
                {YC},
                {RC},
                {S},
                {BONUS},
                {BPS},
                {influence},
                {creativity},
                {threat},
                {ict_index}
            ),""".format(
                code=player['code'],
                id=player['id'],
                first_name=player['first_name'].replace("'", " "),
                second_name=player['second_name'].replace("'", " "),
                team_id=player['team'],
                team_code=player['team_code'],
                chance_of_playing_next_round=sanitized_input(player['chance_of_playing_next_round']),
                chance_of_playing_this_round=sanitized_input(player['chance_of_playing_this_round']),
                element_type=player['element_type'],
                ep_next=float(player['ep_next']),
                ep_this=float(player['ep_this']),
                form=float(player['form']),
                now_cost=player['now_cost'],
                point_per_game=float(player['points_per_game']),
                total_points=player['total_points'],
                transfers_in=player['transfers_in'],
                transfers_in_event=player['transfers_out'],
                transfers_out=player['transfers_in_event'],
                transfers_out_event=player['transfers_out_event'],
                value_form=float(player['value_form']),
                value_season=float(player['value_season']),
                minute_play=player['minutes'],
                GS=player['goals_scored'],
                A=player['assists'],
                CS=player['clean_sheets'],
                GC=player['goals_conceded'],
                OG=player['own_goals'],
                PS=player['penalties_saved'],
                PM=player['penalties_missed'],
                YC=player['yellow_cards'],
                RC=player['red_cards'],
                S=player['saves'],
                BONUS=player['bonus'],
                BPS=player['bps'],
                influence=float(player['influence']),
                creativity=float(player['creativity']),
                threat=float(player['threat']),
                ict_index=float(player['ict_index'])
            )

            rows += row
            # print(rows)
        
        sql_query = """
            INSERT INTO PLAYER_CURRENT_STATUS VALUES
            {}
            ON CONFLICT (CODE) DO UPDATE SET
            (
                code,
                id,
                first_name,
                second_name,
                team_id,
                team_code,
                chance_of_playing_next_round,
                chance_of_playing_this_round,
                element_type,
                ep_next,
                ep_this,
                form,
                now_cost,
                point_per_game,
                total_points,
                transfers_in,
                transfers_in_event,
                transfers_out,
                transfers_out_event,
                value_form,
                value_season,
                minute_play,
                GS,
                A,
                CS,
                GC,
                OG,
                PS,
                PM,
                YC,
                RC,
                S,
                BONUS,
                BPS,
                influence,
                creativity,
                threat,
                ict_index
            ) = 
            (
                Excluded.code,
                Excluded.id,
                Excluded.first_name,
                Excluded.second_name,
                Excluded.team_id,
                Excluded.team_code,
                Excluded.chance_of_playing_next_round,
                Excluded.chance_of_playing_this_round,
                Excluded.element_type,
                Excluded.ep_next,
                Excluded.ep_this,
                Excluded.form,
                Excluded.now_cost,
                Excluded.point_per_game,
                Excluded.total_points,
                Excluded.transfers_in,
                Excluded.transfers_in_event,
                Excluded.transfers_out,
                Excluded.transfers_out_event,
                Excluded.value_form,
                Excluded.value_season,
                Excluded.minute_play,
                Excluded.GS,
                Excluded.A,
                Excluded.CS,
                Excluded.GC,
                Excluded.OG,
                Excluded.PS,
                Excluded.PM,
                Excluded.YC,
                Excluded.RC,
                Excluded.S,
                Excluded.BONUS,
                Excluded.BPS,
                Excluded.influence,
                Excluded.creativity,
                Excluded.threat,
                Excluded.ict_index
            );
        """.format(rows[:-1]) # remove the last comma
        ti.xcom_push('user_data', sql_query)
    
    extrack_task = PythonOperator(
        task_id='extract_player_current_status',
        python_callable=extract_player_current_status
    )

    load_task = PostgresOperator(
        task_id='upsert_player_current_status',
        postgres_conn_id='postgres',
        database='fpl',
        sql="{{ ti.xcom_pull(task_ids='extract_player_current_status', key='user_data') }}"
    )
    
    extrack_task >> load_task

