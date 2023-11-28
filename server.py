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
from rq import Queue, Worker
from poll_boxscore_task import poll_boxscore
from rq_scheduler import Scheduler
from healthcheck import HealthCheck
from decimal import Decimal
from threading import Thread
from pydantic import BaseModel
from typing import List
from typing import Dict
from azure.cosmos import CosmosClient, PartitionKey

import redis

logging.basicConfig(
  stream=sys.stderr,
  level=logging.INFO,
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
COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT")
COSMOS_KEY = os.getenv("COSMOS_KEY")
COSMOS_DB_NAME = os.getenv("COSMOS_DB_NAME")
COSMOS_GAMES_CONTAINER_NAME = os.getenv("COSMOS_GAMES_CONTAINER_NAME")
COSMOS_PLAYERS_CONTAINER_NAME = os.getenv("COSMOS_PLAYERS_CONTAINER_NAME")
COSMOS_PARTITION_KEY = os.getenv("COSMOS_PARTITION_KEY")
# AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
# AZURE_QUEUE_NAME = os.getenv("AZURE_QUEUE_NAME")
REDIS_HOST = os.getenv("REDIS_HOST")
logger.info(f"Redis host: {REDIS_HOST}")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_USERNAME = os.getenv("REDIS_USERNAME")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
logger.info(f"Redis password: {REDIS_PASSWORD}")
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

# rps_contract_factory_abi = None
# rps_contract_abi = None
cosmos_games_container = None
cosmos_players_container = None
# queue_client = None
# gas_oracle = []
ethereum_prices = []

health = HealthCheck()
app.add_url_rule("/healthcheck", "healthcheck", view_func=lambda: health.run())

redis_client = redis.Redis(host=REDIS_HOST, 
                  port=REDIS_PORT, 
                  username=REDIS_USERNAME, 
                  password=REDIS_PASSWORD,
                  ssl=True, ssl_cert_reqs=None)

def eth_to_usd(eth_balance):
  latest_price = ethereum_prices[-1] #get_eth_price()
  eth_price = Decimal(latest_price)  # convert the result to Decimal
  return eth_balance * eth_price

def start_worker_for_queue(queue_name):
  worker = Worker([Queue(queue_name, connection=redis_client)], connection=redis_client)
  worker.work()
 
def schedule_task_with_dedicated_worker(game_id, run_at):
  # Create a scheduler instance and schedule the task
  scheduler = Scheduler(queue_name=game_id, connection=redis_client)
  
  scheduler.enqueue_at(run_at, poll_boxscore, game_id, timeout=-1)
 
  # Start a dedicated worker for this queue
  p = Process(target=start_worker_for_queue, args=(game_id, ))
  p.start()
  return p

class Player(BaseModel):
  id: str
  games: List[str]

class Square(BaseModel):
  id: str
  home_points: int
  away_points: int
  player_id: str

class ScoringPlay(BaseModel):
  id: str
  type: str
  play_type: str
  home_points: int
  away_points: int

class Game(BaseModel):
  id: str
  contract_address: str
  name: str
  scheduled: str
  players: List[str]
  claimed_squares: List[Square]
  payouts: List[Square]
  scoring_plays: List[ScoringPlay]

def send_to_all_except(event, message, game, player_id):
  for player in game['players'].values():
    if player['player_id'] != player_id:
      room = f"{player['player_id']}-{game['game_id']}"
      logger.info(f"Sending {event} to room: {room}")
      emit(event, message, room=room)

@app.route('/scoring-play/<game_id>', methods=['POST'])
def scoring_play(game_id):
  data = request.get_json()
  scoring_plays = data['scoring_plays']
  home_team = data['home_team']
  away_team = data['away_team']
  logger.info(f"Number of scoring plays received for game id: {game_id}: {len(scoring_plays)}")

  for scoring_play in scoring_plays:
    logger.info(f"Scoring play type: \"{scoring_play['type']}\" for game id: {game_id} with play id: {scoring_play['id']}")
    logger.info(f"Scoring play play_type: \"{scoring_play['play_type']}\" for game id: {game_id} with play id: {scoring_play['id']}")
    logger.info(f"Scoring play home points: {scoring_play['home_points']} for game id: {game_id} with play id: {scoring_play['id']}")
    logger.info(f"Scoring play away points: {scoring_play['away_points']} for game id: {game_id} with play id: {scoring_play['id']}")
    logger.info(f"Home team: {home_team} for game id: {game_id} with play id: {scoring_play['id']}")
    logger.info(f"Away team: {away_team} for game id: {game_id} with play id: {scoring_play['id']}")
  # game = games[game_id]
  # player = game['players'][data['player_id']]

  # message = {
  #   'game': game,
  #   'player': player,
  #   'scoring_play': data['scoring_play']
  # }

  # send_to_all_except('scoring_play', message, game, data['player_id'])

  return jsonify({ 'success': True })

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
  game = None

  global games_directory
  if data['game_id'] in games_directory.games:
    game = games_directory.games.get(data['game_id'])
  else:
    logger.error(f"Game {data['game_id']} does not exist.")
    emit('game_not_found', data['player_id'])
    return

  global player_directory
  if data['player_id'] in player_directory.players:
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
  logger.info('Games for current week...')

  response = requests.get(f"{SPORTRADAR_URL}?api_key={SPORTRADAR_API_KEY}")
  games_directory_data = {}

  if response.status_code == 200:
    data = response.json()
    sportradar_game_list = data['week']['games']

    for game in sportradar_game_list:
      if game['status'] != 'closed' and game['status'] != 'inprogress':
        name = f"{game['home']['name']} vs {game['away']['name']}"
        scheduled = game['scheduled']
      
        new_game = {
          'game_id': game['id'],
          'name': name,
          'scheduled': scheduled,
          'players': [],
          'claimed_squares': []
        }

        games_directory_data['games'] = {
            game['id']: new_game
        }

    return GamesDirectory(**games_directory_data)
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
  game = games_directory.get(data['game_id'])
  del game.claimed_squares[data['square_id']]

  logger.info(f"Game state after square unclaimed: {game.json()}")

@socketio.on('claim_square')
def handle_claim_square(data):
  logger.info(f"Received claim square from client: {data}")
  game = games_directory.get(data['game_id'])
  game.claimed_squares.append(Square(**data['square']))

  logger.info(f"Game state after square claimed: {game.json()}")

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

  player_directory_data = {
    'players': {
      player_id: Player(game_ids=[])
    }
  }
  
  global player_directory
  player_directory = PlayerDirectory(**player_directory_data)

  logger.info(f"Player {player_id} connected.")

  join_room(player_id)

  global games_directory
  games_list = [{"game_id": game["game_id"], "name": game["name"]} for game in games_directory.games]

  emit('connected', { 'games_list': games_list, 'player_id': player_id }, room=player_id)

def schedule_games(games):
  processes = []
  for game in games:    
    run_at = datetime.fromisoformat(game["scheduled"])
    logger.info(f"Scheduling job for game id: {game['game_id']} at {run_at}")
    p = schedule_task_with_dedicated_worker(game["game_id"], run_at)
    logger.info(f"Scheduled job for game id: {game['game_id']} at {run_at}")
    processes.append(p)

  # Write each pid from the processes array to file
  with open('worker_pids.txt', 'a') as file:
    for p in processes:
      file.write(f"{p.pid}\n")
      
  # Optionally, wait for all processes
  # for p in processes:
  #   p.join()

  # consumer = EventHubConsumerClient.from_connection_string(
  #   conn_str=EVENTHUB_CONNECTION_STR, 
  #   eventhub_name=EVENTHUB_NAME, 
  #   consumer_group="$Default")
    
  # with consumer:
  #   consumer.receive(on_event=on_event, starting_position="-1")
def create_cosmos_games_container():
  client = CosmosClient(url=COSMOS_ENDPOINT, credential=COSMOS_KEY)
  database = client.create_database_if_not_exists(id=COSMOS_DB_NAME)
  key_path = PartitionKey(path=COSMOS_PARTITION_KEY)

  return database.create_container_if_not_exists(
    id=COSMOS_GAMES_CONTAINER_NAME,
    partition_key=key_path,
    offer_throughput=400
  )

def create_cosmos_players_container():
  client = CosmosClient(url=COSMOS_ENDPOINT, credential=COSMOS_KEY)
  database = client.create_database_if_not_exists(id=COSMOS_DB_NAME)
  key_path = PartitionKey(path=COSMOS_PARTITION_KEY)

  return database.create_container_if_not_exists(
    id=COSMOS_PLAYERS_CONTAINER_NAME,
    partition_key=key_path,
    offer_throughput=400
  )

if __name__ == '__main__':
  from geventwebsocket.handler import WebSocketHandler
  from gevent.pywsgi import WSGIServer

  cosmos_games_container = create_cosmos_games_container()
  cosmos_players_container = create_cosmos_players_container()

  guids = [str(uuid.uuid4()) for _ in range(3)]

  games = [
    {"scheduled": "2023-11-28T06:40:00+00:00", "game_id": str(guids[0])},
    {"scheduled": "2023-11-28T06:42:00+00:00", "game_id": str(guids[1])},
    {"scheduled": "2023-11-28T06:44:00+00:00", "game_id": str(guids[2])}
  ]

  scheduling_thread = Thread(target=schedule_games, args=(games, ))
  scheduling_thread.start()

  # receiver_thread = Thread(target=receive_events, args=(EVENTHUB_CONNECTION_STR, EVENTHUB_NAME, ))
  # receiver_thread.start()

  logger.info('Starting server...')

  # games_directory = get_games_for_current_week()

  http_server = WSGIServer(('0.0.0.0', 443),
                           app,
                           keyfile=KEYFILE,
                           certfile=CERTFILE,
                           handler_class=WebSocketHandler)

  http_server.serve_forever()  
