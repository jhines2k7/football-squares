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
    'id': str(uuid.uuid4()),
    'name': name,
    'players': {},
  }

def get_new_player():
  return {
    'id': str(uuid.uuid4()),
    'game_id': None,
  }

def send_to_all_except(event, message, game, player_id):
  for player in game['players'].values():
    if player['id'] != player_id:
      emit(event, message, room=player['id'])

@socketio.on('join_game')
def join_game(data):
  game = games[data['game_id']]

  if data['player_id'] in game['players']:
    logger.info(f"Player {data['player_id']} already joined game.")
    return
  
  player = players[data['player_id']]
  player['game_id'] = game['id']

  game['players'][data['player_id']] = player
  logger.info(f"Game info: {game}")

  logger.info(f"Player {player['id']} joined game.")
  emit('game_joined', game, room=player['id'])
  
  logger.info(f"Game info: {game}")

  join_room(game['id'])

def generate_games():
  logger.info('Generating games...')
  for i in range(0, 9):
    game = get_new_game(names[i])
    games[game['id']] = game   

@socketio.on('heartbeat')
def handle_heartbeat(data):
  logger.info('Received heartbeat from client.')
  logger.info(data)
  # Emit a response back to the client
  emit('heartbeat_response', 'pong')

@socketio.on('claim_square')
def handle_claim_square(data):
  logger.info(f"Received claim square from client: {data}")
  game = games[data['game_id']]

  message = {
    'row': data['row'],
    'column': data['column'],
    'claimed_by': data['player_id'],
  }

  send_to_all_except('square_claimed', message, game, data['player_id'])

@socketio.on('connect')
def handle_connect():
  logger.info('Client connected.')
  player = get_new_player()
  players[player['id']] = player
  logger.info(f"Player info: {player}")

  join_room(player['id'])

  games_list = [{"game_id": game["id"], "name": game["name"]} for game in games.values()]

  emit('connected', { 'games_list': games_list, 'player': player }, room=player['id'])

  # join_game(player)

if __name__ == '__main__':
  from geventwebsocket.handler import WebSocketHandler
  from gevent.pywsgi import WSGIServer
  
  logger.info('Starting server...')

  generate_games()

  # http_server = WSGIServer(('0.0.0.0', 443),
  #                          app,
  #                          keyfile=KEYFILE,
  #                          certfile=CERTFILE,
  #                          handler_class=WebSocketHandler)

  http_server = WSGIServer(('0.0.0.0', 8000), app, handler_class=WebSocketHandler)

  http_server.serve_forever()  
