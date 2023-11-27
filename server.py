from gevent import monkey
monkey.patch_all()

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
import requests

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload

from flask import Flask, jsonify, request
from flask_socketio import SocketIO, join_room, emit, leave_room
from flask_cors import CORS
from web3 import Web3
from web3.middleware import geth_poa_middleware
from eth_account import Account
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

from datetime import datetime
from multiprocessing import Process, Value
from redis import Redis
from rq import Queue, Worker
from poll_boxscore_task import poll_boxscore
from rq_scheduler import Scheduler
from healthcheck import HealthCheck
from decimal import Decimal
from azure.eventhub import EventHubConsumerClient

logging.basicConfig(
  stream=sys.stderr,
  level=logging.DEBUG,
  format='%(levelname)s:%(asctime)s:%(message)s'
)

logger = logging.getLogger(__name__)

# Creating a logger that will write transaction hashes to a file
txn_logger = logging.getLogger('txn_logger')
txn_logger.setLevel(logging.CRITICAL)

# Creates a file handler which writes DEBUG messages or higher to the file
# Get current date and time
now = datetime.now()

# Format datetime string to be used in the filename
dt_string = now.strftime("%d_%m_%Y_%H_%M_%S")

log_file_name = 'txn_hashes_{}.log'.format(dt_string)
logger.info(f"Log file name: {log_file_name}")
log_file_handler = RotatingFileHandler('logs/' + log_file_name, maxBytes=1e6, backupCount=50)
log_file_handler.setLevel(logging.CRITICAL)

# Creates a formatter and adds it to the handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_file_handler.setFormatter(formatter)

# Adds the handler to the logger
txn_logger.addHandler(log_file_handler)

# Create a parser for the command-line arguments
# e.g. python your_script.py --env .env.prod
parser = argparse.ArgumentParser(description='Loads variables from the specified .env file and prints them.')
parser.add_argument('--env', type=str, default='.env.ganache', help='The .env file to load')
args = parser.parse_args()
# Load the .env file specified in the command-line arguments
load_dotenv(args.env)
# HTTP_PROVIDER = os.getenv("HTTP_PROVIDER")
# CONTRACT_OWNER_PRIVATE_KEY = os.getenv("CONTRACT_OWNER_PRIVATE_KEY")
# RPS_CONTRACT_ADDRESS = os.getenv("RPS_CONTRACT_ADDRESS")
# logger.info(f"RPS contract address: {RPS_CONTRACT_ADDRESS}")
KEYFILE = os.getenv("KEYFILE")
CERTFILE = os.getenv("CERTFILE")
# COINGECKO_API = os.getenv("COINGECKO_API")
# GAS_ORACLE_API_KEY = os.getenv("GAS_ORACLE_API_KEY")
# COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT")
# COSMOS_KEY = os.getenv("COSMOS_KEY")
# COSMOS_DB_NAME = os.getenv("COSMOS_DB_NAME")
# COSMOS_CONTAINER_NAME = os.getenv("COSMOS_CONTAINER_NAME")
# AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
# AZURE_QUEUE_NAME = os.getenv("AZURE_QUEUE_NAME")
# COSMOS_PARTITION_KEY = os.getenv("COSMOS_PARTITION_KEY")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_USERNAME = os.getenv("REDIS_USERNAME")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
SPORTRADAR_URL = os.getenv("SPORTRADAR_URL")
SPORTRADAR_API_KEY = os.getenv("SPORTRADAR_API_KEY")
EVENTHUB_CONNECTION_STR = os.getenv("EVENTHUB_CONNECTION_STR")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")

# Connection
# web3 = Web3(Web3.HTTPProvider(HTTP_PROVIDER))
# if 'sepolia' in args.env or 'mainnet' in args.env:
#   web3.middleware_onion.inject(geth_poa_middleware, layer=0)

if 'ganache' in args.env:
  logger.info("Running on Ganache testnet")
elif 'sepolia' in args.env:
  logger.info("Running on Sepolia testnet")
elif 'mainnet' in args.env:
  logger.info("Running on Ethereum mainnet")

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv("SECRET_KEY")
CORS(app)
socketio = SocketIO(app, async_mode='gevent', cors_allowed_origins='*')

rps_contract_factory_abi = None
rps_contract_abi = None
cosmos_db = None
queue_client = None
gas_oracles = []
ethereum_prices = []

health = HealthCheck()
app.add_url_rule("/healthcheck", "healthcheck", view_func=lambda: health.run())

games = {}
players = {}

redis_conn = Redis(host=REDIS_HOST, 
                    port=REDIS_PORT, 
                    username=REDIS_USERNAME, 
                    password=REDIS_PASSWORD,
                    ssl=True, ssl_cert_reqs=None)

def eth_to_usd(eth_balance):
  latest_price = ethereum_prices[-1] #get_eth_price()
  eth_price = Decimal(latest_price)  # convert the result to Decimal
  return eth_balance * eth_price

def on_event(partition_context, event):
  # Print the event data
  print("Received event from partition: {}".format(partition_context.partition_id))
  print("Event data: {}".format(event.body_as_str()))

def receive_events(event_hub_connection_str, event_hub_name):
  consumer = EventHubConsumerClient.from_connection_string(
    conn_str=event_hub_connection_str, 
    eventhub_name=event_hub_name, 
    consumer_group="$Default")
    
  with consumer:
    consumer.receive(on_event=on_event, starting_position="-1")

receive_events(EVENTHUB_CONNECTION_STR, EVENTHUB_NAME)

def start_worker_for_queue(queue_name):
  worker = Worker([Queue(queue_name, connection=redis_conn)], connection=redis_conn)
  worker.work()
 
def schedule_task_with_dedicated_worker(game_id, run_at):
  # Create a scheduler instance and schedule the task
  scheduler = Scheduler(queue_name=game_id, connection=redis_conn)
  
  scheduler.enqueue_at(run_at, poll_boxscore, game_id)

  # Start a dedicated worker for this queue
  p = Process(target=start_worker_for_queue, args=(game_id, ))
  p.start()
  return p

def get_new_game(name, id, scheduled):
  return {
    'game_id': id,
    'name': name,
    'scheduled': scheduled,
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

def get_games_for_current_week():
  games = {}
  
  logger.info('Games for current week...')

  response = requests.get(f"{SPORTRADAR_URL}?api_key={SPORTRADAR_API_KEY}")

  if response.status_code == 200:
    data = response.json()
    # logger.info(data)

    sportradar_game_list = data['week']['games']

    for game in sportradar_game_list:
      if game['status'] != 'closed' and game['status'] != 'inprogress':
        name = f"{game['home']['name']} vs {game['away']['name']}"
        scheduled = game['scheduled']
      
        new_game = get_new_game(name, game['id'], scheduled)
        games[new_game['game_id']] = new_game
    
    return games
  else:
    logger.error(f"Request failed with status code {response.status_code}")

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
  from geventwebsocket.handler import WebSocketHandler
  from gevent.pywsgi import WSGIServer
  
  logger.info('Starting server...')

  games = get_games_for_current_week()

  processes = []
  for game in games.values():    
    run_at = datetime.fromisoformat(game["scheduled"])
    print(f"Scheduling job for game id: {game['game_id']} at {run_at}")
    p = schedule_task_with_dedicated_worker(game["game_id"], run_at)
    processes.append(p)

  # Write each pid from the processes array to file
  with open('worker_pids.txt', 'a') as file:
    for p in processes:
      file.write(f"{p.pid}\n")
      
  # Optionally, wait for all processes
  for p in processes:
    p.join()

  http_server = WSGIServer(('0.0.0.0', 443),
                           app,
                           keyfile=KEYFILE,
                           certfile=CERTFILE,
                           handler_class=WebSocketHandler)

  http_server.serve_forever()  
