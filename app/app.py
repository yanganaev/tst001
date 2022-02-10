from flask import Flask, request, render_template
from prometheus_flask_exporter import PrometheusMetrics
from markupsafe import escape
from cpu_load_generator import load_all_cores
import mariadb
import nhltop

app = Flask(__name__)
metrics = PrometheusMetrics(app)

# prevent cached responses
@app.after_request
def add_header(response):
    response.cache_control.no_cache = True
    response.cache_control.no_store = True

    return response

## Main page
@app.route('/')
def rt_main():
    # Try to connect to DB server
    try:
        db_conn = nhltop.db_connect()
    except mariadb.Error as err:
        return render_template('msg.j2', title = 'Database error', 
                                message = f'<p>Error no: {err.errno}, msg: {err.msg}</p>')

    # Update schema if needed
    nhltop.db_update_schema(db_conn)

    seasons = nhltop.db_get_seasons(db_conn)
    content = ''

    if not seasons:
        db_conn.close()
        return render_template('msg.j2', title = 'Database empty', 
                                message = """<p>The database is empty. Click <a href="/update/">here</a>
                                             to fetch the data from the NHL API.</p>""")

    for season in seasons:
        content = content + f'<h2>Season: {str(season)[:4]}-{str(season)[4:]}</h2>\n'

        top_players = nhltop.db_get_top_players(db_conn, season)

        for player in top_players['players']:
            gamePk = player['gamePk']
            personId = player['personId']
            fullName = player['fullName']

            content = content + f'<p><a href="/stats?gamePk={gamePk}&personId={personId}">'
            content = content + f'{fullName}</a></p>'

    db_conn.close()
    return render_template('main.j2', c=content)

# Health check page
@app.route('/check/')
def rt_check():
    return 'ok'

# CPU burning routine (to initiate autoscaling)
@app.route('/cpuburn/<int:seconds>')
@app.route('/cpuburn/')
def cpu_burn(seconds = 60):
    load_all_cores(duration_s=seconds, target_load=1.0)
    return render_template('msg.j2', title = 'CPU burner', message = 'CPU stress complete')

# DB update page
@app.route('/update/<int:count>')
@app.route('/update/')
def rt_update(count = 3):
    # Try to connect to DB server
    try:
        db_conn = nhltop.db_connect()
    except mariadb.Error as err:
        return render_template('msg.j2', title = 'Database error',
                                message = f'<p>Error no: {err.errno}, msg: {err.msg}</p>')

    # Update schema if needed
    nhltop.db_update_schema(db_conn)

    if (count < 1):
        count = 1

    if (count > 15) and (count < 20062007):
        count = 15

    if count <= 15:
        seasons = nhltop.get_last_seasons(count)
    else:
        seasons = [ str(count) ]

    for season in seasons:
        all_stars_games = nhltop.get_season_games(season, 'A')
        playoff_games = nhltop.get_season_games(season, 'P')

        final_games = []
        for game in playoff_games:
            if str(game['gamePk'])[7] == '4':
                final_games.append(game)

        for game in all_stars_games + final_games:
            players = nhltop.get_game_players(game['gamePk'])
            nhltop.db_store_game(db_conn, game)
            for p in players:
                nhltop.db_store_player_stat(db_conn, game, p)

    db_conn.close()
    return render_template('msg.j2', title = 'Database updated',
                            message = """<p>Database is updated.
                              <a href="/">Return to the main page</a> to view.</p>""")

# Player statistics page
@app.route('/stats', methods=['GET'])
def rt_stats():
    # Try to connect to DB server
    try:
        db_conn = nhltop.db_connect()
    except mariadb.Error as err:
        error_text = f'<p>Error no: {err.errno}, msg: {err.msg}</p>'
        render_template('msg.j2', title = 'DB error', message = error_text)

    # Update schema if needed
    nhltop.db_update_schema(db_conn)

    # Parse arguments
    gamePk = request.args.get('gamePk', 0, type=int)
    personId = request.args.get('personId', 0, type=int)

    # Fetch statistics from DB
    game_stat = nhltop.db_get_game(db_conn, gamePk)
    player_stat = nhltop.db_get_player_stat(db_conn, personId, gamePk)

    # Fill template with data
    body = render_template('stats.j2', g=game_stat, p=player_stat)

    db_conn.close()

    return body


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
