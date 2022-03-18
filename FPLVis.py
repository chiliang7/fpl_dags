#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np

import plotly.express as px
import plotly.graph_objects as go
import sqlalchemy.engine as sql_engine


# In[2]:


user_name = None
password  = None

alchemyEngine = sql_engine.create_engine("postgresql://{}:{}@localhost/fpl".format(user_name, password))
dbConn = alchemyEngine.connect()


# ## Points-Per-Game-Per-Million-Pound-Last-Five vs Points-Per-Game-Per-Million-Pound-Season

# In[3]:


df = pd.read_sql("""
SELECT
	t1.first_name,
	t1.second_name,
	t3.short_name as team_short_name,
	Round(t1.point_per_game / t1.now_cost, 2) as points_per_game_per_million_pound_season,
	Round(t2.avg_points / t1.now_cost, 2) as points_per_game_per_million_pound_last_five,
    t1.total_points
FROM player_current_status as t1 join
(
	SELECT player_code, round(avg(points), 2) as avg_points
	FROM player_histories
	WHERE minutes_played > 0 and event >= 25
	GROUP BY player_code
	) as t2 on t1.code = t2.player_code
	JOIN teams as t3 on t1.team_id = t3.id
WHERE t1.total_points >= 30 AND t3.short_name in ('CHE', 'TOT')
ORDER BY short_name, points_per_game_per_million_pound_season desc
""", dbConn)
fig = px.scatter(df,
    x='points_per_game_per_million_pound_last_five',
    y='points_per_game_per_million_pound_season',
    color='team_short_name',
    size='total_points',
    title='Value Charts (Bubble Size Indicate Total Points)',
    hover_data=['first_name', 'second_name'])

fig.add_annotation(x=0.18, y=0.1, showarrow=True, arrowhead=1, text='Chalobah')
fig.add_annotation(x=0.29, y=0.1, showarrow=True, arrowhead=1, text='James')
fig.add_annotation(x=0.14, y=0.09, showarrow=True, arrowhead=1, text='Doherty')
fig.add_annotation(x=0.1, y=0.09, showarrow=True, arrowhead=1, text='Kulusevki')
fig.add_annotation(x=0.11, y=0.05, showarrow=True, arrowhead=1, text='Havertz')
fig.add_annotation(x=0.05, y=0.06, showarrow=True, arrowhead=1, text='Son')
fig.add_annotation(x=0.06, y=0.04, showarrow=True, arrowhead=1, text='Kane')
fig.add_annotation(x=0.01, y=0.03, showarrow=True, arrowhead=1, text='Lukaku')


# ## Form vs Total Points Season to Day

# In[4]:


df = pd.read_sql("""
SELECT
	t1.first_name,
	t1.second_name,
	t3.short_name as team_short_name,
	t1.total_points,
	t2.last_5_total_points,
	t1.now_cost
FROM player_current_status as t1 join
(
	SELECT player_code, sum(points) as last_5_total_points
	FROM player_histories
	WHERE minutes_played > 0 and event >= 25
	GROUP BY player_code
	) as t2 on t1.code = t2.player_code
JOIN teams as t3 on t1.team_id = t3.id
WHERE t3.short_name in ('TOT', 'CHE') and total_points >= 30
ORDER BY short_name
""", dbConn)

fig = px.scatter(df,
    x='last_5_total_points',
    y='total_points',
    color='team_short_name',
    hover_data=['first_name', 'second_name'],
    size='now_cost',
    title='Performance Chart (Bubble Size Indicate Cost)')

fig.add_annotation(x=55, y=123, showarrow=True, arrowhead=1, text='Kane')
fig.add_annotation(x=35, y=150, showarrow=True, arrowhead=1, text='Son')
fig.add_annotation(x=44, y=45, showarrow=True, arrowhead=1, text='Kulusevski')
fig.add_annotation(x=35, y=85, showarrow=True, arrowhead=1, text='Havertz')
fig.add_annotation(x=41, y=50, showarrow=True, arrowhead=1, text='Doherty')
fig.add_annotation(x=19, y=123, showarrow=True, arrowhead=1, text='Mount')
fig.add_annotation(x=17, y=119, showarrow=True, arrowhead=1, text='Rüdiger')
fig.add_annotation(x=5, y=71, showarrow=True, arrowhead=1, text='Moura')
fig.add_annotation(x=4, y=63, showarrow=True, arrowhead=1, text='Lukaku')


# ## Spurs Players Minutes Played

# In[5]:


df = pd.read_sql("""
WITH TEMP_TABLE AS (
    SELECT T1.second_name, T2.event, SUM(T2.minutes_played) AS minutes_played
    FROM
    (
        SELECT T1.first_name, T1.second_name, t1.code, T2.short_name, T1.total_points
        FROM player_current_status AS T1
            JOIN teams AS T2 ON T1.team_id = T2.ID
    ) AS T1
    JOIN player_histories AS T2 ON T1.CODE = T2.player_CODE
    WHERE T1.short_name = 'TOT' AND T1.total_points >= 50 AND T1.second_name IN ('Doherty', 'Kane', 'Son', 'Sánchez', 'Davies')
    group by T1.second_name, t2.event
    ORDER BY T1.second_name, T2.event
)
SELECT event, second_name, minutes_played
FROM temp_table
""", dbConn)

px.line(df,
    x='event',
    y='minutes_played',
    facet_col='second_name',
    title='Select Spurs Players Minutes Played in Past Game Weeks'
)


# ## Chelsea Players Minutes Played

# In[6]:


df = pd.read_sql("""
WITH TEMP_TABLE AS (
    SELECT T1.second_NAME, T2.event, SUM(T2.MINUTES_PLAYED) AS MINUTES_PLAYED
    FROM
    (
        SELECT T1.first_name, T1.second_name, t1.code, T2.SHORT_NAME, T1.TOTAL_POINTS
        FROM PLAYER_CURRENT_STATUS AS T1
            JOIN TEAMS AS T2 ON T1.TEAM_ID = T2.ID
    ) AS T1
    JOIN PLAYER_HISTORIES AS T2 ON T1.CODE = T2.player_CODE
    WHERE T1.SHORT_NAME = 'CHE' AND T1.total_points >= 80 and second_name in ('Chalobah', 'Mendy', 'Havertz', 'James', 'Rüdiger')
    group by T1.second_name, t2.EVENT
    ORDER BY T1.second_NAME, T2.EVENT
)
SELECT EVENT, second_NAME, MINUTES_PLAYED
FROM TEMP_TABLE

""", dbConn)
px.line(df, x='event', y='minutes_played', facet_col='second_name', title='Select Chelsea Players Minutes Played in Past Game Weeks')


# ## Fixture Difficulties

# In[7]:


fixture_difficulty = pd.read_sql("""
    SELECT
        week,
        T2.SHORT_NAME,
        team,
        difficulty,
        round((difficulty + next_week_diff + week_2_later_diff) / 3.0, 2) as avg_diff_next_3_weeks,
        round((difficulty + next_week_diff + week_2_later_diff + week_3_later_diff + week_4_later_diff) / 5.0, 2) as avg_diff_next_5_weeks
    FROM
    (
        SELECT
            week,
            team,
            difficulty,
            lead(difficulty, 1) over (partition by t1.team order by week) as next_week_diff,
            lead(difficulty, 2) over (partition by t1.team order by week) as week_2_later_diff,
            lead(difficulty, 3) over (partition by t1.team order by week) as week_3_later_diff,
            lead(difficulty, 4) over (partition by t1.team order by week) as week_4_later_diff
        FROM
        (
            SELECT TEAM, WEEK, ROUND(AVG(difficulty), 2) AS difficulty
            FROM
            (
                SELECT event as week, h_team as team, h_team_diff as difficulty
                from fixtures
                union
                select event as week, a_team as team, a_team_diff as difficulty
                from fixtures
                order by team, week
                ) as t1
            GROUP BY TEAM, WEEK
            ) as t1
        ) as t1 JOIN TEAMS AS T2 ON T1.TEAM = T2.ID
    where week = 31
""", dbConn)


# In[8]:


def interpolated_color(value):
    r = np.interp(value, [1, 5], [255, 255])
    g = np.interp(value, [1, 5], [255, 80])
    b = np.interp(value, [1, 5], [255, 80])

    return 'rgb({}, {}, {})'.format(r, g, b)


def interpolated_colors(row):
    colors = [
        'rgb(255, 255, 255)',
        interpolated_color(row[1]),
        interpolated_color(row[2]),
        interpolated_color(row[3])
    ]

    return colors


# In[9]:


height = 450
sorted_fixture_difficulty = fixture_difficulty[['short_name', 'difficulty', 'avg_diff_next_3_weeks', "avg_diff_next_5_weeks"]].sort_values('difficulty')[:10]


cell_colors = sorted_fixture_difficulty[['short_name', 'difficulty', 'avg_diff_next_3_weeks', "avg_diff_next_5_weeks"]].apply(lambda x: interpolated_colors(x), axis = 1, result_type = 'broadcast')

fig1 = go.Figure(data=[
    go.Table(
        header=dict(
            values=[
                '<b>Team</b>',
                '<b>Difficulty</b>',
                '<b>Avg. Difficulty Next 3 Weeks</b>',
                '<b>Avg. Difficulty Next 5 Weeks</b>'
            ]
        ),
        cells=dict(
            values=sorted_fixture_difficulty[['short_name', 'difficulty', 'avg_diff_next_3_weeks', "avg_diff_next_5_weeks"]].transpose(),
            fill_color=cell_colors.values.transpose()

        )
    )
], layout={ 'height' : height })


sorted_fixture_difficulty = fixture_difficulty[['short_name', 'difficulty', 'avg_diff_next_3_weeks', "avg_diff_next_5_weeks"]].sort_values('avg_diff_next_3_weeks')[:10]

cell_colors = sorted_fixture_difficulty[['short_name', 'difficulty', 'avg_diff_next_3_weeks', "avg_diff_next_5_weeks"]].apply(lambda x: interpolated_colors(x), axis = 1, result_type = 'broadcast')

fig2 = go.Figure(data=[
    go.Table(
        header=dict(
            values=[
                '<b>Team</b>',
                '<b>Difficulty</b>',
                '<b>Avg. Difficulty Next 3 Weeks</b>',
                '<b>Avg. Difficulty Next 5 Weeks</b>'
            ]
        ),
        cells=dict(
            values=sorted_fixture_difficulty[['short_name', 'difficulty', 'avg_diff_next_3_weeks', "avg_diff_next_5_weeks"]].transpose(),
            fill_color=cell_colors.values.transpose()

        )
    )
], layout={ 'height': height })

sorted_fixture_difficulty = fixture_difficulty[['short_name', 'difficulty', 'avg_diff_next_3_weeks', "avg_diff_next_5_weeks"]].sort_values('avg_diff_next_5_weeks')[:10]

cell_colors = sorted_fixture_difficulty[['short_name', 'difficulty', 'avg_diff_next_3_weeks', "avg_diff_next_5_weeks"]].apply(lambda x: interpolated_colors(x), axis = 1, result_type = 'broadcast')

fig3 = go.Figure(data=[
    go.Table(
        header=dict(
            values=[
                '<b>Team</b>',
                '<b>Difficulty</b>',
                '<b>Avg. Difficulty Next 3 Weeks</b>',
                '<b>Avg. Difficulty Next 5 Weeks</b>'
            ]
        ),
        cells=dict(
            values=sorted_fixture_difficulty[['short_name', 'difficulty', 'avg_diff_next_3_weeks', "avg_diff_next_5_weeks"]].transpose(),
            fill_color=cell_colors.values.transpose()

        )
    )
], layout={ 'height' : height })

fig1.update_layout(title_text='Sort by Avg Difficulty of Next Week')
fig2.update_layout(title_text='Sort by Avg Difficulty of Next 3 Weeks')
fig3.update_layout(title_text='Sort by Avg Difficulty of Next 5 Weeks')


fig1.show()
fig2.show()
fig3.show()


# ## Initial Player Selection By Most Net Transfers

# In[10]:


df = pd.read_sql("""
select *
from (
    SELECT
        case T1.ELEMENT_TYPE
        when 1 then 'Goal Keeper'
        when 2 then 'Defender'
        when 3 then 'Midfielder'
        when 4 then 'Forward'
        end as player_type,
        T1.FIRST_NAME,
        T1.SECOND_NAME,
        T2.SHORT_NAME,
        value_form,
        value_season,
        total_points,
        now_cost,
        transfers_in - transfers_out as net_transfers,
        rank() over (partition by element_type order by (transfers_in - transfers_out) desc) as group_rank
    FROM PLAYER_CURRENT_STATUS as T1
        join TEAMS as T2 ON T1.TEAM_ID = T2.ID
    ) as t1
where group_rank <= 10
""", dbConn)


# In[11]:


goal_keeper_color = 'rgb(102, 204, 255)'
defender_color    = 'rgb(204, 255, 204)'
midfielder_color  = 'rgb(255, 255, 204)'
forward_color     = 'rgb(255, 204, 255)'

def fill_color(x):
    colors = None;

    if x[0] == 'Goal Keeper':
        colors = [goal_keeper_color] * 8
    elif x[0] == 'Defender':
        colors = [defender_color] * 8
    elif x[0] == 'Midfielder':
        colors = [midfielder_color] * 8
    else:
        colors = [forward_color] * 8

    return colors

df['formatted_net_transfers'] = df['net_transfers'].apply(lambda x: f'{x:,}')
cell_colors = df[['player_type', 'first_name', 'second_name', 'short_name', 'total_points', 'now_cost', 'net_transfers', 'group_rank']].apply(lambda x: fill_color(x), axis = 1, result_type='broadcast')

fig = go.Figure(data=[
    go.Table(
        header=dict(
            values=[
                '<b>Player Type</b>',
                '<b>First Name</b>',
                '<b>Seccond Name</b>',
                '<b>Team</b>',
                '<b>Total Points</b>',
                '<b>New Cost</b>',
                '<b>Net Transfers</b>',
                '<b>Group Rank</b>'
            ]
        ),
        cells=dict(
            values=df[[
                'player_type',
                'first_name',
                'second_name',
                'short_name',
                'total_points',
                'now_cost',
                'formatted_net_transfers',
                'group_rank'
            ]].transpose(),
            fill_color=cell_colors.values.transpose()
        ),

    )
], layout={ 'height' : 1250 })



fig.show()


# In[12]:


dbConn.close()

