#!/usr/bin/env python

import nhltop

def test_get_with_retries():
    reply = nhltop.get_with_retries('https://statsapi.web.nhl.com/api/v1/languages')
    assert reply[0]['languageCode'] == 'en'

def test_get_last_seasons():
    seasons = nhltop.get_last_seasons(3)
    assert len(seasons) == 3
    assert seasons[0][:2] == '20'

def test_get_season_games():
   games = nhltop.get_season_games('20182019', 'A')
   assert len(games) == 3
   assert games[0]['gamePk'] == 2018040641

def test_get_game_players():
    assert len(nhltop.get_game_players(2018040643)) == 22
