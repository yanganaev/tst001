#!/usr/bin/env python
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import requests
import sys
import os
import mariadb

# Wrap requests.get function to survive long site reply delays
def get_with_retries(url):
    # timeout in seconds
    timeout = 5

    retry_strategy = Retry(
        total=10,
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor = 0.1
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)

    try:
        reply = http.get(url, timeout=timeout)
        reply.raise_for_status()
        return reply.json()
    except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as err:
        return {}
    except requests.exceptions.RequestException as err:
        print(f'{err}')
        return {}

# Get last N finished NHL seasons (no more than 15)
def get_last_seasons(count):
    result = []

    # Get no more than 15 seasons, and no less than 1
    if count < 1: count = 1
    if count > 15: count = 15

    baseurl = 'https://statsapi.web.nhl.com/api/v1/seasons/'

    reply = get_with_retries(baseurl + 'current')
    if reply == {}:
        return []

    current_season = reply['seasons'][0]['seasonId']

    reply = get_with_retries(baseurl)
    if reply == {}:
        return []

    if reply['seasons'][-1]['seasonId'] == current_season:
        last = -2
    else:
        last = -1
    for idx in range(last-count+1, last+1):
        result.append(reply['seasons'][idx]['seasonId'])

    return result

# returns list of season games of particular type (A or P)
def get_season_games(season, type):
    result = []
    baseurl = 'https://statsapi.web.nhl.com/api/v1/schedule?'

    reply = get_with_retries(baseurl + 'season=' + season + '&gameType=' + type)
    if reply == {}:
        return []

    for date in reply['dates']:
      for game in date['games']:
        game['gameDate'] = date['date']  # replace gameDate with correct date (start of the game)
        result.append(game)

    return result

# returns list of players which took part in the game
def get_game_players(game_id):
    result = []
    baseurl = 'https://statsapi.web.nhl.com/api/v1/game/'

    reply = get_with_retries(baseurl + str(game_id) + '/boxscore')
    if reply == {}:
        return []
    
    team_away = reply['teams']['away']['team']
    team_home = reply['teams']['home']['team']
    players_away = reply['teams']['away']['players']
    players_home = reply['teams']['home']['players']

    for key in players_away.keys():
        player = players_away[key]
        if player['stats']: 
            player['team'] = team_away
            result.append(player)

    for key in players_home.keys():
        player = players_home[key]
        if player['stats']: 
            player['team'] = team_home
            result.append(player)

    return result

# Database connect (errors are handled in calling functions)
def db_connect():
    username = os.environ.get('DB_USER')
    password = os.environ.get('DB_PASSWORD')
    host = os.environ.get('DB_HOST')
    database = os.environ.get('DB_NAME')

    return mariadb.connect(
        username = username,
        password = password,
        host = host,
        database = database
    )

# Update DB schema if needed (for now just recreate all the tables)
def db_update_schema(conn):
    required_version = 1
    try:
        cur = conn.cursor()
        cur.execute('SELECT version from schema_ver')
        for (version,) in cur:
            if version != required_version:
                version = 0
    except mariadb.Error as err:
        if err.errno == 1146:
            # Table not found - database is probably ampty
            version = 0
        else:
            raise err

    if version == 0:
        # DB init
        cur.execute("""
            DROP TABLE IF EXISTS
              schema_ver,
              goalieStats,
              skaterStats,
              players,
              games
            """)
        conn.commit()

        cur.execute("""
            CREATE TABLE games (
              gamePk INT UNSIGNED NOT NULL PRIMARY KEY, 
              season INT UNSIGNED,
              gameType CHAR,
              gameDate DATE,
              team_away_id SMALLINT UNSIGNED,
              team_away_name NVARCHAR(255),
              team_away_score TINYINT UNSIGNED,
              team_home_id SMALLINT UNSIGNED,
              team_home_name NVARCHAR(255),
              team_home_score TINYINT UNSIGNED
            )""")

        cur.execute("""
            CREATE TABLE players (
              gamePk INT UNSIGNED NOT NULL, 
              personId INT UNSIGNED NOT NULL,
              fullName NVARCHAR(255),
              birthDate DATE,
              birthCity NVARCHAR(50),
              birthCountry NVARCHAR(10),
              nationality NVARCHAR(10),
              jerseyNumber TINYINT UNSIGNED,
              positionName NVARCHAR(30),
              teamName NVARCHAR(50),
              teamId SMALLINT UNSIGNED,
              PRIMARY KEY(gamePk, personId),
              CONSTRAINT `fk_gamePk`
                 FOREIGN KEY (gamePk) REFERENCES games (gamePk)
                 ON DELETE CASCADE
                 ON UPDATE CASCADE
            )""")


        cur.execute("""
            CREATE TABLE goalieStats (
              gamePk INT UNSIGNED NOT NULL,
              personId INT UNSIGNED NOT NULL,
              timeOnIce NVARCHAR(10),
              assists SMALLINT,
              goals SMALLINT,
              pim SMALLINT,
              shots SMALLINT,
              saves SMALLINT,
              powerPlaySaves SMALLINT,
              shortHandedSaves SMALLINT,
              evenSaves SMALLINT,
              shortHandedShotsAgainst SMALLINT,
              evenShotsAgainst SMALLINT,
              powerPlayShotsAgainst SMALLINT,
              savePercentage DECIMAL(17,14),
              PRIMARY KEY(gamePk, personId),
              CONSTRAINT `fk_game_person_g`
                 FOREIGN KEY (gamePk, personId) REFERENCES players (gamePk, personId)
                 ON DELETE CASCADE
                 ON UPDATE CASCADE
            )""")


        cur.execute("""
            CREATE TABLE skaterStats (
              gamePk INT UNSIGNED NOT NULL,
              personId INT UNSIGNED NOT NULL,
              timeOnIce NVARCHAR(10),
              assists SMALLINT,
              goals SMALLINT,
              shots SMALLINT,
              hits SMALLINT,
              powerPlayGoals SMALLINT,
              powerPlayAssists SMALLINT,
              penaltyMinutes SMALLINT,
              faceOffWins SMALLINT,
              faceoffTaken SMALLINT,
              takeaways SMALLINT,
              giveaways SMALLINT,
              shortHandedGoals SMALLINT,
              shortHandedAssists SMALLINT,
              blocked SMALLINT,
              plusMinus SMALLINT,
              evenTimeOnIce NVARCHAR(10),
              powerPlayTimeOnIce NVARCHAR(10),
              shortHandedTimeOnIce NVARCHAR(10),
              PRIMARY KEY(gamePk, personId),
              CONSTRAINT `fk_game_person_s`
                 FOREIGN KEY (gamePk, personId) REFERENCES players (gamePk, personId)
                 ON DELETE CASCADE
                 ON UPDATE CASCADE
            )""")

        cur.execute("""
            CREATE TABLE schema_ver(
              version SMALLINT UNSIGNED NOT NULL PRIMARY KEY
            )""")
        cur.execute('INSERT INTO schema_ver (version) VALUES (1)')
        conn.commit()

# Store game details to database
def db_store_game(conn, game):
    cur = conn.cursor()
    cur.execute("""
        REPLACE INTO games (
           gamePk,
           season,
           gameType,
           gameDate,
           team_away_id,
           team_away_name,
           team_away_score,
           team_home_id,
           team_home_name,
           team_home_score
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
           game['gamePk'],
           game['season'],
           game['gameType'],
           game['gameDate'],
           game['teams']['away']['team']['id'],
           game['teams']['away']['team']['name'],
           game['teams']['away']['score'],
           game['teams']['home']['team']['id'],
           game['teams']['home']['team']['name'],
           game['teams']['home']['score']
        )
    )

    conn.commit()

# Store player statistics to the database
def db_store_player_stat(conn, game, player):
    cur = conn.cursor()

    # Save personal info
    cur.execute("""
        REPLACE INTO players (
           gamePk,
           personId,
           fullName,
           birthDate,
           birthCity,
           birthCountry,
           nationality,
           jerseyNumber,
           positionName,
           teamName,
           teamId
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
           game['gamePk'],
           player['person']['id'],
           player['person']['fullName'],
           player['person']['birthDate'],
           player['person']['birthCity'],
           player['person']['birthCountry'],
           player['person']['nationality'],
           player['jerseyNumber'],
           player['position']['name'],
           player['team']['name'],
           player['team']['id']
        )
    )
    # Save goalie stats
    if player['position']['name'] == 'Goalie':
        # Check for missing keys
        player['stats']['goalieStats']['savePercentage'] = player['stats']['goalieStats'].get('savePercentage', 0)
        # Save data
        cur.execute("""
            REPLACE INTO goalieStats (
               gamePk,
               personId,
               timeOnIce,
               assists,
               goals,
               pim,
               shots,
               saves,
               powerPlaySaves,
               shortHandedSaves,
               evenSaves,
               shortHandedShotsAgainst,
               evenShotsAgainst,
               powerPlayShotsAgainst,
               savePercentage
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
               game['gamePk'],
               player['person']['id'],
               player['stats']['goalieStats']['timeOnIce'],
               player['stats']['goalieStats']['assists'],
               player['stats']['goalieStats']['goals'],
               player['stats']['goalieStats']['pim'],
               player['stats']['goalieStats']['shots'],
               player['stats']['goalieStats']['saves'],
               player['stats']['goalieStats']['powerPlaySaves'],
               player['stats']['goalieStats']['shortHandedSaves'],
               player['stats']['goalieStats']['evenSaves'],
               player['stats']['goalieStats']['shortHandedShotsAgainst'],
               player['stats']['goalieStats']['evenShotsAgainst'],
               player['stats']['goalieStats']['powerPlayShotsAgainst'],
               player['stats']['goalieStats']['savePercentage']
            )
        )
    # Save skater stats
    else:
        # Check for missing keys
        player['stats']['skaterStats']['hits'] = player['stats']['skaterStats'].get('hits', 0)
        player['stats']['skaterStats']['takeaways'] =  player['stats']['skaterStats'].get('takeaways', 0)
        player['stats']['skaterStats']['giveaways'] =  player['stats']['skaterStats'].get('giveaways', 0)
        player['stats']['skaterStats']['blocked'] = player['stats']['skaterStats'].get('blocked', 0)

        # Save
        cur.execute("""
            REPLACE INTO skaterStats (
               gamePk,
               personId,
               timeOnIce,
               assists,
               goals,
               shots,
               hits,
               powerPlayGoals,
               powerPlayAssists,
               penaltyMinutes,
               faceOffWins,
               faceoffTaken,
               takeaways,
               giveaways,
               shortHandedGoals,
               shortHandedAssists,
               blocked,
               plusMinus,
               evenTimeOnIce,
               powerPlayTimeOnIce,
               shortHandedTimeOnIce
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
               game['gamePk'],
               player['person']['id'],
               player['stats']['skaterStats']['timeOnIce'],
               player['stats']['skaterStats']['assists'],
               player['stats']['skaterStats']['goals'],
               player['stats']['skaterStats']['shots'],
               player['stats']['skaterStats']['hits'],
               player['stats']['skaterStats']['powerPlayGoals'],
               player['stats']['skaterStats']['powerPlayAssists'],
               player['stats']['skaterStats']['penaltyMinutes'],
               player['stats']['skaterStats']['faceOffWins'],
               player['stats']['skaterStats']['faceoffTaken'],
               player['stats']['skaterStats']['takeaways'],
               player['stats']['skaterStats']['giveaways'],
               player['stats']['skaterStats']['shortHandedGoals'],
               player['stats']['skaterStats']['shortHandedAssists'],
               player['stats']['skaterStats']['blocked'],
               player['stats']['skaterStats']['plusMinus'],
               player['stats']['skaterStats']['evenTimeOnIce'],
               player['stats']['skaterStats']['powerPlayTimeOnIce'],
               player['stats']['skaterStats']['shortHandedTimeOnIce']
            )
        )

    conn.commit()

# Returns a list of seasons stored in the database
def db_get_seasons(conn):
    cur = conn.cursor()
    result = []

    cur.execute('SELECT DISTINCT season FROM games') 

    for (season,) in cur:
        result.append(season)

    return result

# Retrieve players, who played both All-stars and Final games of the season
def db_get_top_players(conn, season):
    cur = conn.cursor()
    result = {'players': []}

    cur.execute("""
        WITH q1 AS
         (SELECT p.personId,
                 p.gamePk,
                 g.gameType,
                 g.season 
          FROM players p INNER JOIN games g ON p.gamePk = g.gamePk WHERE g.gameType = 'P'),
        q2 AS
         (SELECT p.personId,
                 p.gamePk, 
                 g.gameType,
                 g.season 
         FROM players p INNER JOIN games g ON p.gamePk = g.gamePk WHERE g.gameType = 'A')
        SELECT DISTINCT
          q1.personId,
          q1.season
        FROM q1 INNER JOIN q2 ON q1.personId = q2.personId AND q1.season = q2.season
        WHERE q1.season = ?
        """,
        (season,)
    )

    players = []
    for (personId, season) in cur:
        players.append(personId)

    for player in players:
        cur.execute("""
            SELECT p.personId, p.gamePk, p.fullName, g.gameType, g.season 
            FROM players p INNER JOIN games g ON p.gamePk = g.gamePk
            WHERE g.gameType = 'P' AND p.personId = ? AND g.season = ?
            ORDER BY g.gamePk DESC LIMIT 1""",
            (player, season)
        )
        for (personId, gamePk, fullName, gameType, season) in cur:
            result['players'].append({'personId': personId, 'fullName': fullName, 'gamePk': gamePk})

    return result

# Retrieves game details from the database
def db_get_game(conn, gamePk):
    cur = conn.cursor()
    result = {}

    cur.execute("SELECT * FROM games WHERE gamePk = ?", (gamePk,))
    for (gamePk, season, gameType, gameDate, 
         team_away_id, team_away_name, team_away_score,
         team_home_id, team_home_name, team_home_score) in cur:

        result['gameDate'] = gameDate
        result['team_away_name'] = team_away_name
        result['team_away_score'] = team_away_score
        result['team_home_name'] = team_home_name
        result['team_home_score'] = team_home_score

    return result

# Retrieves player statistics from the database
def db_get_player_stat(conn, personId, gamePk):
    cur = conn.cursor()
    result = {}

    cur.execute("""
            SELECT fullName, birthDate, birthCity, birthCountry,
                   nationality, jerseyNumber, positionName, teamName
            FROM players
            WHERE personId = ? AND gamePk = ?""",
            (personId, gamePk)
    )

    for (fullName, birthDate, birthCity, birthCountry,
         nationality, jerseyNumber, positionName, teamName) in cur:

        result['fullName'] = fullName
        result['birthDate'] = birthDate
        result['birthCity'] = birthCity
        result['birthCountry'] = birthCountry
        result['nationality'] = nationality
        result['jerseyNumber'] = jerseyNumber
        result['positionName'] = positionName
        result['teamName'] = teamName

        if positionName == 'Goalie':
            # Get goalie stat
            cur.execute("""SELECT * FROM goalieStats 
                           WHERE personId = ? AND gamePk = ?""",
                        (personId, gamePk))
            result['goalieStats'] = {}
            for (gamePk, personId, timeOnIce, assists, goals, pim, shots, saves,
                  powerPlaySaves, shortHandedSaves, evenSaves,
                  shortHandedShotsAgainst, evenShotsAgainst,
                  powerPlayShotsAgainst, savePercentage ) in cur:
                result['goalieStats']['timeOnIce'] = timeOnIce
                result['goalieStats']['assists'] = assists
                result['goalieStats']['goals'] = goals
                result['goalieStats']['pim'] = pim
                result['goalieStats']['shots'] = shots
                result['goalieStats']['saves'] = saves
                result['goalieStats']['powerPlaySaves'] = powerPlaySaves
                result['goalieStats']['shortHandedSaves'] = shortHandedSaves
                result['goalieStats']['evenSaves'] = evenSaves
                result['goalieStats']['shortHandedShotsAgainst'] = shortHandedShotsAgainst
                result['goalieStats']['evenShotsAgainst'] = evenShotsAgainst
                result['goalieStats']['powerPlayShotsAgainst'] = powerPlayShotsAgainst
                result['goalieStats']['savePercentage'] = savePercentage
        else:
            # Get skater stat
            cur.execute("""SELECT * FROM skaterStats 
                           WHERE personId = ? AND gamePk = ?""",
                        (personId, gamePk))
            result['skaterStats'] = {}
            for (gamePk, personId, timeOnIce, assists, goals, shots, hits,
                 powerPlayGoals, powerPlayAssists, penaltyMinutes, faceOffWins,
                 faceoffTaken, takeaways, giveaways, shortHandedGoals,
                 shortHandedAssists, blocked, plusMinus, evenTimeOnIce,
                 powerPlayTimeOnIce, shortHandedTimeOnIce) in cur:
                result['skaterStats']['timeOnIce'] = timeOnIce
                result['skaterStats']['assists'] = assists
                result['skaterStats']['goals'] = goals
                result['skaterStats']['shots'] = shots
                result['skaterStats']['hits'] = hits
                result['skaterStats']['powerPlayGoals'] = powerPlayGoals
                result['skaterStats']['powerPlayAssists'] = powerPlayAssists
                result['skaterStats']['penaltyMinutes'] = penaltyMinutes
                result['skaterStats']['faceOffWins'] = faceOffWins
                result['skaterStats']['faceoffTaken'] = faceoffTaken
                result['skaterStats']['takeaways'] = takeaways
                result['skaterStats']['giveaways'] = giveaways
                result['skaterStats']['shortHandedGoals'] = shortHandedGoals
                result['skaterStats']['shortHandedAssists'] = shortHandedAssists
                result['skaterStats']['blocked'] = blocked
                result['skaterStats']['plusMinus'] = plusMinus
                result['skaterStats']['evenTimeOnIce'] = evenTimeOnIce
                result['skaterStats']['powerPlayTimeOnIce'] = powerPlayTimeOnIce
                result['skaterStats']['shortHandedTimeOnIce'] = shortHandedTimeOnIce

    return result

# Entry point if called from cli
if __name__ == "__main__":
    try:
        arg = sys.argv[1]
    except IndexError:
        arg = 'display'

    # Try to connect to DB server
    try:
        db_conn = db_connect()
    except mariadb.Error as err:
        print(f'Error no: {err.errno}, msg: {err.msg}')
        exit(1)

    # Update schema if needed
    db_update_schema(db_conn)

    if arg == 'update':
        seasons = get_last_seasons(3)

        for season in seasons:
            all_stars_games = get_season_games(season, 'A')
            playoff_games = get_season_games(season, 'P')

            final_games = []
            for game in playoff_games:
                if str(game['gamePk'])[7] == '4':
                    final_games.append(game)

            for game in all_stars_games + final_games:
                players = get_game_players(game['gamePk'])
                db_store_game(db_conn, game)
                for p in players:
                    db_store_player_stat(db_conn, game, p)

    else:
        seasons = db_get_seasons(db_conn)

        print('Players, who took part both in All-stars and Final games:')

        for season in seasons:
            print(f'Season: {season}')

            top_players = db_get_top_players(db_conn, season)
            for player in top_players['players']:
                print(db_get_player_stat(db_conn, player['personId'], player['gamePk']))
                print(db_get_game(db_conn, player['gamePk']), '\n')

