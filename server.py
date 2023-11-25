from gevent import monkey
monkey.patch_all()

import sys
import logging
import uuid
import json
import argparse
import os
import math
import queue
import binascii
import requests
import asyncio
import aiohttp

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload

# from flask import Flask, jsonify, request
from quart import Quart, request, jsonify
from flask_socketio import SocketIO, join_room, emit, leave_room
from flask_cors import CORS
from web3 import Web3
from web3.middleware import geth_poa_middleware
from eth_account import Account
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
from datetime import datetime

logging.basicConfig(
  stream=sys.stderr,
  level=logging.DEBUG,
  format='%(levelname)s:%(asctime)s:%(message)s'
)

logger = logging.getLogger(__name__)

app = Quart(__name__)
# app.config['SECRET_KEY'] = 'your-secret-key'
CORS(app)
socketio = SocketIO(app, async_mode='gevent', cors_allowed_origins='*')

games = {}
players = {}
start_times = []

def get_new_game(name, id):
  return {
    'game_id': id,
    'name': name,
    'players': {},
    'claimed_squares': {},
  }

def get_new_player(player_id=None):
  return {
    'player_id': player_id,
    'games': {}
    # {
    #   'game_name': game['name'],
    #   'claimed_squares': []
    # }
  }

def send_to_all_except(event, message, game, player_id):
  for player in game['players'].values():
    if player['player_id'] != player_id:
      room = f"{player['player_id']}-{game['game_id']}"
      logger.info(f"Sending {event} to room: {room}")
      emit(event, message, room=room)

@app.route('/games', methods=['GET'])
def get_games():
  player_id = request.args.get('player_id')

  if player_id in players:
    games_list = [{"game_id": game["game_id"], "name": game["name"]} for game in games.values()]
    
    return jsonify(games_list)

@socketio.on('leave_game')
def leave_game(data):
  game = games[data['game_id']]
  player = game['players'][data['player_id']]

  message = {
    'game': game,
    'player': player
  }

  send_to_all_except('player_left_game', message, game, data['player_id'])

  logger.info(f"Player {player['player_id']} left game: {game}")
  logger.info(f"Squares claimed in game {data['game_id']} before unclaim: {game['claimed_squares']}")

  # unclaim player squares in game
  for square in player['games'][data['game_id']]['claimed_squares']:
    del game['claimed_squares'][f"{square['row']}{square['column']}"]

  logger.info(f"Squares claimed in game {data['game_id']} after unclaim: {game['claimed_squares']}")

  leave_room(game['game_id'])
  leave_room(f"{player['player_id']}-{game['game_id']}")

  del player['games'][data['game_id']]
  del game['players'][data['player_id']]

@socketio.on('join_game')
def join_game(data):
  try:
    game = games[data['game_id']]
    # handle KeyError if game does not exist
  except KeyError:
    # if game does not exist, emit an event to the client
    logger.info(f"Game {data['game_id']} does not exist.")
    emit('game_not_found', data['player_id'])
    return

  if data['player_id'] in game['players']:
    logger.info(f"Player {data['player_id']} already joined game.")
    join_room(f"{data['player_id']}-{game['game_id']}")
    emit('game_joined', game, room=data['player_id'])
    return
  
  player = players[data['player_id']]
  player['games'][data['game_id']] = {
    'game_name': game['name'],
    'claimed_squares': []
  }

  game['players'][data['player_id']] = player

  logger.info(f"Player {player['player_id']} joined game: {game}")  
  logger.info(f"Join game Game info: {game}")

  join_room(game['game_id'])
  join_room(f"{player['player_id']}-{game['game_id']}")
  
  emit('game_joined', game, room=f"{player['player_id']}-{game['game_id']}")
  send_to_all_except('new_player_joined', player, game, data['player_id'])

async def get_games_for_current_week():
  logger.info('Games for current week...')

  url = "http://api.sportradar.us/nfl/official/trial/v7/en/games/current_week/schedule.json?api_key=e92psk369hgpspwbu2eysmua"

  response = requests.get(url)

  if response.status_code == 200:
    data = response.json()
    # logger.info(data)

    sportradar_game_list = data['week']['games']

    for game in sportradar_game_list:
      if game['status'] != 'closed' and game['status'] != 'inprogress' and game['status'] != 'created':
        name = f"{game['home']['name']} vs {game['away']['name']}"
      
        new_game = get_new_game(name, game['id'])
        games[new_game['game_id']] = new_game

        start_times.append(game['scheduled'])
  else:
    logger.info(f"Request failed with status code {response.status_code}")

  asyncio.create_task(run_at_start_time(get_box_score_update, "2023-11-25 03:38:09"))
  # for start_time in start_times:
  #   logger.info(f"Start time: {start_time}")
  #   asyncio.create_task(run_at_start_time(get_box_score_update, start_time))

@app.before_serving
async def before_serving():
  await get_games_for_current_week()

async def get_box_score_update():
  url = 'http://api.sportradar.us/nfl/official/trial/v7/en/games/c452d212-f557-4234-83ad-d66fa417d3e5/boxscore.json?api_key=e92psk369hgpspwbu2eysmua'
  async with aiohttp.ClientSession() as session:
    async with session.get(url) as response:
      response_text = await response.text()
      print(response_text)

async def run_at_start_time(func, start_time):
  while True:
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%dT%H:%M:%S%z")
    logger.info(f"Current time: {current_time}, Start time: {start_time}")

    if current_time == start_time:
      logger.info(f"Running function at {current_time}")
      await func()
      await asyncio.sleep(30)
    
    logger.info(f"Sleeping for 5 seconds...")
    await asyncio.sleep(5)

@socketio.on('heartbeat')
def handle_heartbeat(data):
  logger.info('Received heartbeat from client.')
  logger.info(data)
  # Emit a response back to the client
  emit('heartbeat_response', 'pong')

@socketio.on('unclaim_square')
def handle_unclaim_square(data):
  logger.info(f"Unclaiming square: {data}")
  game = games[data['game_id']]
  square = data['square']
  row_col = f"{square['row']}{square['column']}"
  logger.info(f"Unclaiming row_col: {row_col}")
  del game['claimed_squares'][row_col]

  player = game['players'][data['player_id']]
  player['games'][data['game_id']]['claimed_squares'].remove(
    {
      'row': data['square']['row'],
      'column': data['square']['column'],
    }
  )

  logger.info(f"Game state after player: {data['player_id']} unclaimed square: {game}")

@socketio.on('claim_square')
def handle_claim_square(data):
  logger.info(f"Received claim square from client: {data}")
  game = games[data['game_id']]
  game['claimed_squares'].update({f"{data['row']}{data['column']}": data['player_id']})
  
  player = game['players'][data['player_id']]
  player['games'][data['game_id']]['claimed_squares'].append(
    {
      'row': data['row'],
      'column': data['column'],
    }
  )

  logger.info(f"Claim square game info: {game}")

  message = {
    'row': data['row'],
    'column': data['column'],
    'claimed_by': data['player_id'],
    'game_id': data['game_id']
  }

  send_to_all_except('square_claimed', message, game, data['player_id'])

@socketio.on('connect')
def handle_connect():
  logger.info('Client connected.')
  
  player_id = request.args.get('player_id')

  player = get_new_player(player_id)
  players[player['player_id']] = player
  logger.info(f"Connect player info: {player}")

  join_room(player_id)

  games_list = [{"game_id": game["game_id"], "name": game["name"]} for game in games.values()]

  emit('connected', { 'games_list': games_list, 'player': player }, room=player['player_id'])

if __name__ == '__main__':
  import logging
  from hypercorn.config import Config
  from hypercorn.asyncio import serve

  logger = logging.getLogger('quart.serving')
  logger.setLevel(logging.INFO)
  logger.info('Starting server...')

  # SSL configuration
  config = Config()
  config.bind = ["0.0.0.0:443"]
  config.keyfile = '/etc/letsencrypt/live/fs.generalsolutions43.com/privkey.pem'
  config.certfile = '/etc/letsencrypt/live/fs.generalsolutions43.com/fullchain.pem'
  
  # Run the server
  app.run(debug=True, certfile=config.certfile, keyfile=config.keyfile)
  # from geventwebsocket.handler import WebSocketHandler
  # from gevent.pywsgi import WSGIServer
  
  # logger.info('Starting server...')

  # get_games_for_current_week()

  # http_server = WSGIServer(('0.0.0.0', 443),
  #                          app,
  #                          keyfile='/etc/letsencrypt/live/fs.generalsolutions43.com/privkey.pem',
  #                          certfile='/etc/letsencrypt/live/fs.generalsolutions43.com/fullchain.pem',
  #                          handler_class=WebSocketHandler)

  # http_server = WSGIServer(('0.0.0.0', 8000), app, handler_class=WebSocketHandler)

  # http_server.serve_forever()

