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
from models.football_squares import ScoringPlay, ScoringPlayDTO, Square, Game, Player

import redis
from azure.cosmos.exceptions import CosmosResourceNotFoundError

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
COSMOS_GAMES_PARTITION_KEY_PATH = os.getenv("COSMOS_GAMES_PARTITION_KEY_PATH")
COSMOS_PLAYERS_PARTITION_KEY_PATH = os.getenv("COSMOS_PLAYERS_PARTITION_KEY_PATH")
# AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
# AZURE_QUEUE_NAME = os.getenv("AZURE_QUEUE_NAME")
REDIS_HOST = os.getenv("REDIS_HOST")
logger.info(f"Redis host: {REDIS_HOST}")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_USERNAME = os.getenv("REDIS_USERNAME")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
logger.info(f"Redis password: {REDIS_PASSWORD}")
SPORTRADAR_CURRENT_WEEK_URL = os.getenv("SPORTRADAR_CURRENT_WEEK_URL")
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

def start_worker_for_queue(queue_name: str):
  worker = Worker([Queue(queue_name, connection=redis_client)], connection=redis_client)
  worker.work()
 
def schedule_task_with_dedicated_worker(game: Game, run_at: datetime):
  # Create a scheduler instance and schedule the task
  scheduler = Scheduler(queue_name=game.id, connection=redis_client)
  
  scheduler.enqueue_at(run_at, poll_boxscore, game.id, game.week_id, timeout=-1)
 
  # Start a dedicated worker for this queue
  p = Process(target=start_worker_for_queue, args=(game.id, ))
  p.start()
  return p

def send_to_all_except(event:str, message:str, game: Game, excluded_player_id:str):
  for player_id in game.players:
    if player_id != excluded_player_id:
      room = f"{player_id}-{game.id}"
      emit(event, message, room=room)

@app.route('/scoring-play/<game_id>', methods=['POST'])
def scoring_play(game_id: str):
  # data = request.get_json()
  logger.info(f"Scoring play data received: {request.get_json()}")
  # scoring_play_dto = ScoringPlayDTO.parse_raw(request.get_json())
  scoring_play_dto = ScoringPlayDTO.model_validate_json(request.get_json())
  logger.info(f"Scoring play dto: {scoring_play_dto.model_dump_json()}")
  scoring_plays = scoring_play_dto.scoring_plays
  logger.info(f"Number of scoring plays received for game id: {game_id}: {len(scoring_plays)}")

  global cosmos_games_container

  try:
    result = cosmos_games_container.read_item(item=scoring_play_dto.game_id, partition_key=scoring_play_dto.week_id)
    game = Game(**result)
    logger.info(f"Scoring plays will be added to game: {game.id}")

    for scoring_play in scoring_plays:
      logger.info(f"Scoring play type: \"{scoring_play.type}\" for game id: {game_id} with play id: {scoring_play.id}")
      logger.info(f"Scoring play play_type: \"{scoring_play.play_type}\" for game id: {game_id} with play id: {scoring_play.id}")
      logger.info(f"Scoring play home points: {scoring_play.home_points} for game id: {game_id} with play id: {scoring_play.id}")
      logger.info(f"Scoring play away points: {scoring_play.away_points} for game id: {game_id} with play id: {scoring_play.id}")
      logger.info(f"Home team: {scoring_play.home_team} for game id: {game_id} with play id: {scoring_play.id}")
      logger.info(f"Away team: {scoring_play.away_team} for game id: {game_id} with play id: {scoring_play.id}")

      game.scoring_plays.append(scoring_play)

    logger.info(f"Game state after adding scoring plays: {game.model_dump_json()}")
    cosmos_games_container.replace_item(item=game.id, body=game.model_dump())
    logger.info(f"Successfully added scoring plays to game: {game.id}")
  except CosmosResourceNotFoundError:
    return jsonify([])

  # game = games[game_id]
  # player = game['players'][data['player_id']]

  # message = {
  #   'game': game,
  #   'player': player,
  #   'scoring_play': data['scoring_play']
  # }

  # send_to_all_except('scoring_play', message, game, data['player_id'])

  return jsonify({ 'success': True })

def get_current_current_games():
  logger.info('Getting current games...')
  global cosmos_games_container
  result = cosmos_games_container.query_items(
    query="SELECT * FROM games WHERE games.status = 'scheduled'",
    enable_cross_partition_query=True
  )

  return [Game(**game) for game in result]

@app.route('/games', methods=['GET'])
def get_games():
  player_id = request.args.get('player_id')
  # using player_id to get a player from the database
  global cosmos_players_container
  player = cosmos_players_container.read_item(item=player_id, partition_key=player_id)

  # only players that have joined a game will be in the database
  if player is None:
    return jsonify([])
  else:
    global cosmos_games_container
    games = cosmos_games_container.query_items(
      query=f"SELECT * FROM c WHERE ARRAY_CONTAINS(c.games '{player_id}')",
      enable_cross_partition_query=True
    )
    return jsonify(games_list = [{"game_id": game["game_id"], "name": game["name"]} for game in games.values()])

@socketio.on('leave_game')
def leave_game(data):
  global cosmos_games_container
  result = cosmos_games_container.read_item(item=data['game_id'], partition_key=data['week_id'])
  game = Game(**result)

  if data['player_id'] in game.get('players'):
    message = {
      'game': game,
    }

    send_to_all_except('player_left_game', message, game, data['player_id'])

    logger.info(f"Player {data['player_id']} left game: {game.id}")
    logger.info(f"Squares claimed in game {game.id} before unclaim: ")
    for square in game.claimed_squares:
      logger.info(square.model_dump_json())

    # unclaim player squares in game
    for square in game.claimed_squares:
      if square.player_id == data['player_id']:
        game.claimed_squares.remove(square)

    logger.info(f"Game state after unclaiming squares: {game.model_dump_json()}")

    leave_room(game['game_id'])
    leave_room(f"{data['player_id']}-{game.id}")

    game.players.remove(data['player_id'])
    logger.info(f"Game state after removing player: {game.model_dump_json()}")

    cosmos_games_container.replace_item(item=game.id, body=game.model_dump_json())

@socketio.on('join_game')
def join_game(data):
  global cosmos_games_container
  result = cosmos_games_container.read_item(item=data['game_id'], partition_key=data['week_id'])
  game = Game(**result)

  if data['player_id'] in game.players:
    logger.info(f"Player {data['player_id']} already joined game.")
    join_room(f"{data['player_id']}-{game.id}")
    emit('game_joined', game, room=data['player_id'])
    return
  
  global cosmos_players_container
  result = cosmos_players_container.read_item(item=data['player_id'], partition_key=data['address'])
  player = Player(**result)
  player.games.append(game.id)
  logger.info(f"Player {player.model_dump_json()} joined game: {game.id}")
  
  cosmos_players_container.replace_item(item=player.id, body=player.model_dump_json())

  game.players.append(player.id)
  cosmos_games_container.replace_item(item=game.id, body=game.model_dump_json())

  logger.info(f"Game state after player {player.id} joined game: {game.model_dump_json()}")

  join_room(game.id)
  join_room(f"{player.id}-{game.id}")
  
  emit('game_joined', game, room=f"{player.id}-{game.id}")
  send_to_all_except('new_player_joined', player, game, player.id)

def save_games_for_current_week():
  logger.info('Games for current week...')

  response = requests.get(f"{SPORTRADAR_CURRENT_WEEK_URL}?api_key={SPORTRADAR_API_KEY}")
  
  global cosmos_games_container

  if response.status_code == 200:
    data = response.json()
    sportradar_game_list = data['week']['games']

    for game in sportradar_game_list:
      if game['status'] != 'closed' and game['status'] != 'inprogress':
        name = f"{game['home']['name']} vs {game['away']['name']}"
        logger.info(f"Saving game: {name}")
        scheduled = game['scheduled']
        logger.info(f"Game scheduled: {scheduled}")

        cosmos_games_container.upsert_item(body={
          'contract_address': '0xC8334AaF263Ec942778c6B04e1dF2d9Bcd08cCa1',
          'id': game['id'],
          'week_id': data['week']['id'],
          'name': name,
          'scheduled': scheduled,
          'players': [],
          'claimed_squares': [],
          'status': game['status'],
          'players': [],
          'claimed_squares': [],
          'payouts': [],
          'scoring_plays': []
        })

        logger.info(f"Game {game['id']} saved: {name}")

@socketio.on('heartbeat')
def handle_heartbeat(data):
  logger.info('Received heartbeat from client.')
  logger.info(data)
  # Emit a response back to the client
  emit('heartbeat_response', 'pong')

@socketio.on('unclaim_square')
def handle_unclaim_square(data):
  logger.info(f"Unclaiming square: {data}")
  square = Square(**data['square'])
  global cosmos_games_container
  result = cosmos_games_container.read_item(item=data['game_id'], partition_key=data['week_id'])
  game = Game(**result)
  game.claimed_squares.remove(square)
  
  cosmos_games_container.replace_item(item=game.id, body=game.model_dump_json())

  logger.info(f"Game state after square unclaimed: {game.json()}")

@socketio.on('claim_square')
def handle_claim_square(data):
  logger.info(f"Received claim square from client: {data}")
  global cosmos_games_container
  result = cosmos_games_container.read_item(item=data['game_id'], partition_key=data['week_id'])
  game = Game(**result)
  game.claimed_squares.append(Square(**data['square']))

  cosmos_games_container.replace_item(item=game.id, body=game.model_dump_json())

  send_to_all_except('square_claimed', data['square'], game, data['player_id'])

@socketio.on('connect')
def handle_connect():
  logger.info('Client connected.')
  
  player_id = request.args.get('player_id')

  player_data = {
    "player": player_id,
    "games": []
  }

  global cosmos_players_container
  cosmos_players_container.create_item(body=player_data)

  logger.info(f"Player {player_id} connected.")

  join_room(player_id)

  global cosmos_games_container
  result = cosmos_games_container.query_items(
    query="SELECT * FROM games WHERE games.status = 'scheduled'",
    enable_cross_partition_query=True
  )
  games = [game for game in result]
  games_list = [{"game_id": game["game_id"], "name": game["name"]} for game in games]

  emit('connected', { 'games_list': games_list, 'player_id': player_id }, room=player_id)

def schedule_games(games: List[Game]):
  processes = []
  for game in games:    
    run_at = datetime.fromisoformat(game.scheduled)
    logger.info(f"Scheduling job for game id: {game.id} at {run_at}")
    p = schedule_task_with_dedicated_worker(game, run_at)
    logger.info(f"Scheduled job for game id: {game.id} at {run_at}")
    processes.append(p)

  # Write each pid from the processes array to file
  with open('worker_pids.txt', 'a') as file:
    for p in processes:
      file.write(f"{p.pid}\n")

def create_cosmos_games_container():
  client = CosmosClient(url=COSMOS_ENDPOINT, credential=COSMOS_KEY)
  database = client.create_database_if_not_exists(id=COSMOS_DB_NAME)
  key_path = PartitionKey(path=COSMOS_GAMES_PARTITION_KEY_PATH)

  return database.create_container_if_not_exists(
    id=COSMOS_GAMES_CONTAINER_NAME,
    partition_key=key_path,
    offer_throughput=400
  )

def create_cosmos_players_container():
  client = CosmosClient(url=COSMOS_ENDPOINT, credential=COSMOS_KEY)
  database = client.create_database_if_not_exists(id=COSMOS_DB_NAME)
  key_path = PartitionKey(path=COSMOS_PLAYERS_PARTITION_KEY_PATH)

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

  save_games_for_current_week()

  week_id = '0796e6a9-84ca-4651-9813-bc8bb391ad95'

  scheduled = [ "2023-11-30T22:04:00+00:00", 
                "2023-11-30T22:05:00+00:00", 
                "2023-11-30T22:06:00+00:00"]
  
  game1 = Game(
    id="6101c145-f366-4918-8642-d7936f38c214",
    week_id=week_id,
    contract_address="", 
    name="Game 1", 
    scheduled=scheduled[0], 
    status="", 
    players=[], 
    claimed_squares=[], 
    payouts=[], 
    scoring_plays=[])
  
  game2 = Game(
    id="4d89a5c1-9d45-40fa-acb9-4af0b43cfe72",
    week_id=week_id,
    contract_address=" ", 
    name="Game 2", 
    scheduled=scheduled[1], 
    status="", 
    players=[], 
    claimed_squares=[], 
    payouts=[], 
    scoring_plays=[])
  
  game3 = Game(
    id="64ef040c-fbfd-4599-8d5f-0af7c00a6f67",
    week_id=week_id,
    contract_address=" ", 
    name="Game 3", 
    scheduled=scheduled[2], 
    status="", 
    players=[], 
    claimed_squares=[], 
    payouts=[], 
    scoring_plays=[])

  games = [game1, game2, game3]

  scheduling_thread = Thread(target=schedule_games, args=(games, ))
  scheduling_thread.start()

  # receiver_thread = Thread(target=receive_events, args=(EVENTHUB_CONNECTION_STR, EVENTHUB_NAME, ))
  # receiver_thread.start()

  logger.info('Starting server...')
  
  http_server = WSGIServer(('0.0.0.0', 443),
                           app,
                           keyfile=KEYFILE,
                           certfile=CERTFILE,
                           handler_class=WebSocketHandler)

  http_server.serve_forever()  
