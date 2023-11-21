import datetime
import sys
import logging
import uuid
import json
import argparse
import os
import math
import queue
import binascii

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
from gevent import monkey
monkey.patch_all()

from flask import Flask, jsonify, request
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

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key'
CORS(app)
socketio = SocketIO(app, async_mode='gevent', cors_allowed_origins='*')

games = {}
players = {}

names = ['Dolphins v Patriots', 
         'Chiefs v Bills', 
         'Buccaneers v Packers', 
         'Rams v Seahawks', 
         'Ravens v Titans', 
         'Bears v Saints', 
         'Browns v Steelers',
         'Colts v Jaguars',
         'Vikings v Lions',
         'Jets v Panthers']

def get_new_game(name):
  return {
    'game_id': str(uuid.uuid4()),
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
  game_id = request.args.get('game_id')
  player_id = request.args.get('player_id')

  if game_id in games:
    game = games[game_id]
    if player_id in game['players']:
      games_list = [{"game_id": game["game_id"], "name": game["name"]} for game in games.values()]
      return jsonify(games_list)

@socketio.on('leave_game')
def leave_game(data):
  game = games[data['game_id']]
  player = game['players'][data['player_id']]

  logger.info(f"Player {player['player_id']} left game: {game}")
  logger.info(f"Leave game: {game}")

  leave_room(game['game_id'])
  leave_room(f"{player['player_id']}-{game['game_id']}")

  # remove the squares claimed by the player
  for square in game['claimed_squares']:
    if game['claimed_squares'][square] == data['player_id']:
      del game['claimed_squares'][square]

  del player['games'][data['game_id']]
  del game['players'][data['player_id']]

  # emit('game_left', game, room=f"{player['player_id']}-{game['game_id']}")
  message = {
    'game': game,
    'player': player
  }

  send_to_all_except('player_left_game', message, game, data['player_id'])
  

@socketio.on('join_game')
def join_game(data):
  game = games[data['game_id']]

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

def generate_games():
  logger.info('Generating games...')
  for i in range(0, 9):
    game = get_new_game(names[i])
    games[game['game_id']] = game   

@socketio.on('heartbeat')
def handle_heartbeat(data):
  logger.info('Received heartbeat from client.')
  logger.info(data)
  # Emit a response back to the client
  emit('heartbeat_response', 'pong')

@socketio.on('unclaim_square')
def handle_unclaim_square(data):
  logger.info(f"Received unclaim square from client: {data}")
  game = games[data['game_id']]
  del game['claimed_squares'][f"{data['row']}{data['column']}"]

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
  from geventwebsocket.handler import WebSocketHandler
  from gevent.pywsgi import WSGIServer
  
  logger.info('Starting server...')

  generate_games()

  http_server = WSGIServer(('0.0.0.0', 443),
                           app,
                           keyfile='/etc/letsencrypt/live/fs.generalsolutions43.com/privkey.pem',
                           certfile='/etc/letsencrypt/live/fs.generalsolutions43.com/fullchain.pem',
                           handler_class=WebSocketHandler)

  # http_server = WSGIServer(('0.0.0.0', 8000), app, handler_class=WebSocketHandler)

  http_server.serve_forever()  
