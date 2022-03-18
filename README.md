# fpl_dags
ETL pipelines that pull the Fantasy Premier League data regularly

<img src=https://user-images.githubusercontent.com/17539049/158665702-1d786210-bd14-4424-9229-a8bce61ad6db.jpg alt="drawing" width="200"/><img src=https://user-images.githubusercontent.com/17539049/158665722-adce5ba2-cba4-44b5-97bc-a7c6635b9b9d.jpg alt="drawing" width="200"/>

# Overview

   A bit of the background on how this project started. My colleagues from London invite me to play the Fantasy Premier League of the 21/22 season. By the time it started, I didn't really know how to play. I applied the basic stats principle to pick my 15-player squad by selecting the top talents in the most prestigious teams like Man City, Man United, Liverpool, Chelsea, and Arsenal. This stragtegy works for a while. Then, I realized there is more than that.


   Over the past few months, I started to get more and more involved in this game and thought about more scientific and efficient ways to manage my team besides just following the social media and FPL show. They are very helpful in general, but I just want to get my hands dirty on the data. I found that you can actually pull the FPL data via their API, which is well documented in this [post](https://medium.com/@frenzelts/fantasy-premier-league-api-endpoints-a-detailed-guide-acbd5598eb19). I started to develop a python jupyter notebook to pull the raw data from the API and dumped them into Postgre database. 


   The data is updated as games are played every weeek, which means I will have to manually trigger my script in the jupyter notebook at the end of every match week. So, I think it will be a very good use case to spin up an Airflow Service locally to take care of this part. In this repo, I am going to walk you through how to set it up and have it pull the FPL data regularily, followed by the analyses of how to play FPL decently.

Before we get started. Here are the prerequisite:
* A mac OS computer. The following tutorial is tested on macs (sorry for windows users). 
* You should have [brew](https://brew.sh/) and [anaconda](https://www.anaconda.com/) install on your machine.

# Setup Standalone Airflow Locally

<details><summary>Click to Expand</summary>
   
Given the different status of where each machine is, it's hard to go through the following procedure to finish the setup without any interruption. It might ask you to upgrade whole bunch stuffs. But here are the basic steps to install apach-airflow

First, create a new environment for this project
   
```bash
$ conda create -n fpl_airflow
```
   
Switch to the fpl environment
```bash
$ conda activate fpl_airflow
```

Then run 
```bash
$ conda install pip
```
to install pip via conda

The run
```bash
$ pip install apache-airflow
```
to install airflow via pip (Note: when I tried to setup on another machine. I also need to upgrade my x-code command line tools. So it's totally normal if your computer requires more steps)

Then install the postgre provider. 
```bash
$ pip install apache-airflow-providers-postgres
```

If everything runs through correctly at this point. You should be able to launch the airflow locally by typing
```bash
$ airflow standalone
```

With the login information listed in the terminal, you can login to the Airflow UI at `0.0.0.0:/8080` or something similar.
   
The default folder for airflow is under `~/airflow`. You should be able to find config. and other files there.

</details>

# Setup Postgresql Locally

<details><summary>Click to Expand</summary>
   
Next, you will need to setup Postgre locally. You can follow [this tutorial](https://wiki.postgresql.org/wiki/Homebrew) to install postgre via brew. After successfully installing the postgre, you can launch it via brew services
```
$ brew services start postgresql
```

If it's your first time to login to postgre server. It probably says that `psql: FATAL: database "user_name" does not exist`. Then you will need to run
```
$ createdb user_name
```

Then you should be able to login via
```
$ psql
```

Assuming you have successfully installed postgre, the next step is creating an user for airflow (Replace the user_name and password respectively). 

```
postgres=# create user user_name with encrypted password 'mypassword';
```

Next, create a database just for the FPL data

```
postgres=# create database fpl
```

You might also need to grant access and other privileges to the user you just created
```
postgres=# grant all privileges on database fpl to user_name;
```

Once the database is setup in the postgre, you need to update the connection so that airflow knows where to connect to the postgre on your machine. The official document has pretty nice writeup [here](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html). Basically, you should fill up a form like this from the Web UI.
![Screen Shot 2022-03-15 at 8 01 31 PM](https://user-images.githubusercontent.com/17539049/158373646-35ab9859-212f-462e-ace0-5913d7612bcb.png)

Lastly, create a function that will update the timestamp when a record is updated.

```
postgres=#
CREATE OR REPLACE FUNCTION trigger_set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = now(); 
   RETURN NEW;
END;
$$ language 'plpgsql';
```

</details>

# Pull FPL Data

<details><summary>Click to Expand</summary>

After the connection is setup, clone the repo under the `airflow/dags` folder and spin up the airflow via
   
```bash
$ airflow standalone
```

you should be able to login to the web UI @ localhost:8080 or something similar and You will see the fpl_dags by typing the `fpl_api` in the search bar. Then we can backfill the data in the command line.

![Screen Shot 2022-03-18 at 4 52 41 AM](https://user-images.githubusercontent.com/17539049/158893333-db0eb6ae-4ab4-4f23-96a0-5ca2baf82a5a.png)


## Initiate the tables via

```bash
$ airflow dags test create_fixture_table '2022-03-15'
```
and 

```bash
$ airflow dags test create_player_tables '2022-03-15'
```
and 
```bash
$ airflow dags test create_team_table '2022-03-15'
```

## Backfill

```bash
$ airflow dags backfill update_fixture_tables --start-date '2022-03-15' --end-date '2022-03-16'
```

and 

```bash
$ airflow dags backfill update_player_histories --start-date '2022-03-15' --end-date '2022-03-16'
```

and 

```bash
$ airflow dags backfill update_player_tables --start-date '2022-03-15' --end-date '2022-03-16'
```

## Schedule

Both `update_player_table` and `update_player_histories` are running at daily cadence so that we can have the fresh data everyday while the `update_fixture_tables` runs on Thursday and Friday because, this is usually when a new week game starts.

</details>

The above tutorial concludes the Airflow setup. The next part will be selecting a team for FPL game and strategies.

# Data Modeling

<details>
   <summary>Click to Expand</summary>
   
Before diving into the analyses, here are what it pulls from the API. (There are other information but I don't think they are particularily useful at the moment). There are only four tables - `Teams, Player_current_status, player_histories, and Fixtures`. Since the amount of data is relatively small. I didn't normalize the table. The fact table is the `player_current_status` with the other 3 as dimension tables. Here is the schema of each one.

## Teams 
* id, 
* name, 
* short_name, 
* strength, 
* strength_overall_away,
* strength_overall_home,
* created_at,
* updated_at


## Player_current_status
Player current status preserve the current snapshot of players' season-to-day stats. such as Goal score.
Here are the full columns
* CODE (PRIMARY KEY) 
* ID (id for this season)
* first_name
* second_name
* team_id (team id for this season)
* team_code (unique identifier for all the Englisher Football Club beyond Premier League)
* chance_of_playing_next_round 
* chance_of_player_this_round
* element_type (type of player a player is: 1 Goal Keeper, 2 Defender, 3 Midfielder, and 4 forward)
* ep_next (expected points next round I believe)
* ep_this (expected points this round)
* form (average point per game over the past 30 days)
* now_cost (how much a player cost right now)
* point_per_game
* total_points
* transfers_in (season-to-day)
* transfers_in_event (transfer this week)
* transfers_out (season-to-day)
* transfers_out_event 
* value_form (form divided by current price)
* value_season (total points divided by current price)
* minute_play 
* GS (goals scored)
* A (assists)
* GC (goals conceded)
* OG (own goal)
* PS (penalty saved)
* PM (penalty missed)
* YC (yellow card)
* RC (red card)
* S (saves)
* Bonus (Bonus points awarded based on the bps)
* [BPS](https://www.premierleague.com/news/106533)
* influence
* creativity
* threat
* ict_index
* created_at
* updated_at

## Player Histories
Schema is similar to player current status. I have full definition [here](https://github.com/chiliang7/fpl_dags/blob/main/create_player_tables.py#L27)

## Fixtures
* code (Primary Key)
* event (game week)
* id (id for this season)
* h_team (home team id)
* h_team_diff (home team difficulty, higher the number, the more adversity the h_team is facing)
* h_score (home team score)
* a_team (away team id)
* a_team_diff (away team difficulty)
* a_score (away team score)

</details>

# The FPL Game

So, the goal of this game is to maximize the points you have from your starting 11 players according to the scoring [rule](https://fantasy.premierleague.com/help/rules) here given the limited budgets. The [prizes](https://fantasy.premierleague.com/prizes) are incredible and very competitive. As of the time I am writing this, there are about 9 million players worldwide to compete. If you decide to play the game for the first time. Here are some tips to select your first squad and how to navigate through the season.

## Initial Squad Selection

<details><summary>Click to Expand</summary>
   
Let's say you don't know about the players at all. A quick way to select your teams is via crowd sourcing. Here is the query result of grouping the players by their type and sorting net transfers descendingly. We can immediately find out the most owned players in each postition. If you pick the top players in each type from this table, here is the 15 players you will have

- Goal Keepers - Ramsdale, De Gea
- Defenders - Cancelo, James, Alexander-Arnold, Rudiger, Alonso
- Midfielders - Jota, Salah, Son, Gallagher, Benrahma
- Forwards - Antonio, Cristano Ronaldo, Dennis

Though they cost 113.9 million pounds, which exceeds the 100 million initial budgets, it's a very good initial pick. Just need a few switches with more budget players, then it should be good to go.

![newplot (21)](https://user-images.githubusercontent.com/17539049/158696364-54a5259d-7c55-4f63-b746-e0d61a94129f.png)

</details>
   
## Navigate Through the Season

A Premier League season starts around September and ends around May next year. It's slightly longer than other professional sport leagues so sometimes it feel bit like a marathon race. To consistently achieve high scores through out the season involves many strategies. The most important one is selecting your players and here are the analyses which I think can inform our decision better.

## Fixture Analysis

<details><summary>Click to Expand</summary>

First, analyzing the fixtures (aka the matches). Given there is only one free transfer every week (every extra transfer will take 4 points away from your total points), this game requires planning weeks ahead. Choose a team with favoring fixtures in the near future will reduce the time that you transfer your players. Rush into the team with good fixture this week might not give you the best results down the road.

Therefore, I use this [query](https://github.com/chiliang7/fpl_dags/blob/main/FPLVis.py#L174) to generate the average difficulty for this week, next-3 or next-5 fixtures to determine if a team is worth investing. 

![newplot (15)](https://user-images.githubusercontent.com/17539049/158542947-09fffdcd-8b02-4864-8f24-3956533f25c9.png)
![newplot (16)](https://user-images.githubusercontent.com/17539049/158542945-60c65927-9c3d-41eb-a403-37d235e2bafa.png)
![newplot (17)](https://user-images.githubusercontent.com/17539049/158542940-aecbddcc-f181-4226-8073-790bc5622ff7.png)

For each table, I take the top 5 teams. if there are ties in the scores, I will take all of them. This gives me following team selections. If we just look at this week's difficulty, It's pretty clear that many teams have good fixture (easy game). However, if we look at next 3 or 5 games together, clearly, there are some teams that really stand out, for example: [Tottenham Hotspur](https://www.tottenhamhotspur.com/) and [Chelsea FC](https://www.chelseafc.com/en)
   
```python
>>> l1 = {'ARS', 'TOT', 'SOU', 'MCI', 'LIV', 'WHU', 'LEE', 'CHE', 'BHA', 'EVE' }
>>> l2 = {'WHU', 'CHE', 'LEE', 'TOT', 'BUR', 'CRY', 'LEI' }
>>> l3 = { 'CHE', 'CRY', 'TOT', 'BUR', 'SOU', 'MCI' }
>>> l1 & l2 & l3
{'TOT', 'CHE'}
>>> l2 & l3
{'CRY', 'BUR', 'TOT', 'CHE'}
```
   
Given the fixture analysis above, we can start prioritizing bringing the players from these two teams. Then, perhaps, consider players from Crystal Palace or Burnley FC.

</details>

## Player Analysis

<details><summary>Click to Expand</summary>
   
Once we decided which team we want to invest in the next few weeks, we need to narrow our selection to 1-3 players from each team (It only allows you to choose 3 players from a football club at most). Before we make the pick, we should at least ask two following questions:
   
### Does this player play?

When picking players, it is important to make sure that they will be in the starting 11 in order to maximize the points. One way to check if they will play is to look at the historical minutes-played of players. Just like many things in this world, the historical pattern do not necessary predict the future. There are factors such as switching head coach, injury, formation change, other games like UEFA CL heavily change the odds of whether or not a player will start. Nonetheless, I think first check how many minutes a player have this season so far isn't a bad start.

Since Chelsea is a big football club with so many games besides Premier League to play throughout the season. It's not surprise that their players' minutes played are all over the place like Chalobah and Havertz. However, on the other defender side, Rüdiger and Mendy are much more consistent and less likely to be benched.
   
![newplot (26)](https://user-images.githubusercontent.com/17539049/158729810-ec048f5a-2de4-4dbd-8b84-237c0f6bcd0d.png)
   
On the other hand, Spurs are not in any Cups competitions or Champion League this season. Their minutes played are much more consistent. Son, Kane, and Davies pretty much start every game while Doherty gains a lot of minutes and Sánchez get more benched recently.
![newplot (24)](https://user-images.githubusercontent.com/17539049/158729481-7a13ea60-18dc-4b77-8347-dfd461ad792b.png)

*I only list 5 players here for explanation purpose. you can definitely chart the whole team with the data that airflow pull*
   
### Is the player worth your money?

First, let's look at the points-per-game-per-million-pound-spent-for-season-to-day vs points-per-game-per-million-pound-spent-for-last-five-matches. Player in the top right corner is a good bargain thoughout all the games they played in this season so far. Reece James and Chalobah falls into category. On the bottom right, we have Havertz, who gains some form recently but not so well in the beginning of the season. The question is that if he can maintain this momentum. 

Kulusevki and Doherty both players are in the starting-11 recently and played very well so it's not surprised to see them to have good values. For the most of the players, they perform pretty consistently overtime so we can see there is a correlation between these two metrics. Lastly, we have Lukaku at bottom left, which does not perform well this season at all and thus, might not be a good pick. 

![newplot (30)](https://user-images.githubusercontent.com/17539049/158740342-43fc5d9a-a1a6-4c7b-b4fb-cff0aa92edff.png)

So, the above method is trying to find out the budget pick. What about if we want to maximize the performance and identify the players who can score most points for their manager? We can use the following total points vs form chart. For Spurs' players, Kane is an obvious pick. Though he is a bit expansive and does not perform well in the beginning of the season. Recently, he definitely imporves and scores a lot of points. Son is a bit lacking the form recently but perform fairly well this season so far, which is still a good pick. Kulusevki and Doherty are also pretty decent given their good forms. Moura should be in the watchlist as he gets benched recently.

As for Chelsea's players, Rüdiger and Mount are good picks. They are consistently deliver the points. Havertz could be a fun pick if he can continue to perform. Lukaku should be in the watchlist as he doesn't play often and not play well, which does not justify his price tag at this moment. 



![newplot (29)](https://user-images.githubusercontent.com/17539049/158740346-79a7e374-bca3-4c58-9307-64dd156d3456.png)

With these analyses, you can apply them to other teams with good fixtures to complete the team selection or when you need to decide whom to transfer in/out during the game.
   
</details>



# Summary

Apache-Airflow is pretty easy to setup locally and is an easy-to-use ETL tool for personal projects. I am satisfied that it pulls the FPL data automatically so I can focus on the analysis part. As for the analyses, the above ones only scratch the surface of the FPL. There are stats like chances created, missed, touches in the box, number of open crosses, shots taken, shots on target ... etc are not included in the dataset but also crucial when determining a player's performance. That being said, there is definitely a lot of room for this project to expand if needed. Simply having these dags to run over years and accumulating player histories and fixtures will be huge. (They aggregate the previous seasons' data, so you can't get the match data from last year). But for now, I think this is good enogh for the rest of the season.


