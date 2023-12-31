import random
import threading
import time
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

from datetime import datetime, timezone
from multiprocessing import Process, Value
from rq import Queue, Worker
from rq_scheduler import Scheduler
from healthcheck import HealthCheck
from decimal import Decimal
from threading import Thread
from pydantic import BaseModel
from typing import Any, List
from typing import Dict
from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import CosmosResourceNotFoundError
from models import ScoringPlay, ScoringPlayDTO, Square, Game, Player

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
# FS_CONTRACT_ADDRESS = os.getenv("FS_CONTRACT_ADDRESS")
KEYFILE = os.getenv("KEYFILE")
CERTFILE = os.getenv("CERTFILE")
COINGECKO_API = os.getenv("COINGECKO_API")
GAS_ORACLE_API_KEY = os.getenv("GAS_ORACLE_API_KEY")
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
SPORTRADAR_BOXSCORE_URL = os.getenv("SPORTRADAR_BOXSCORE_URL")

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

cosmos_games_container = None
cosmos_players_container = None
# queue_client = None
gas_oracles = []
ethereum_prices = []
WEEK_ID = None
fs_contract_abi = None

health = HealthCheck()
app.add_url_rule("/healthcheck", "healthcheck", view_func=lambda: health.run())

redis_client = redis.Redis(host=REDIS_HOST, 
                  port=REDIS_PORT, 
                  username=REDIS_USERNAME, 
                  password=REDIS_PASSWORD,
                  ssl=True, ssl_cert_reqs=None)

def get_service(api_name, api_version, scopes, key_file_location):
  """Get a service that communicates to a Google API.

  Args:
    api_name: The name of the api to connect to.
    api_version: The api version to connect to.
    scopes: A list auth scopes to authorize for the application.
    key_file_location: The path to a valid service account JSON key file.

  Returns:
    A service that is connected to the specified API.
  """

  credentials = service_account.Credentials.from_service_account_file(
  key_file_location)

  scoped_credentials = credentials.with_scopes(scopes)

  # Build the service object.
  service = build(api_name, api_version, credentials=scoped_credentials)

  return service

def download_contract_abi():
  # Define the auth scopes to request.
  scope = 'https://www.googleapis.com/auth/drive.file'
  key_file_location = 'service-account.json'
    
  # Specify the name of the folder you want to retrieve
  folder_name = 'football-squares'
  
  try:
    # Authenticate and construct service.
    service = get_service(
      api_name='drive',
      api_version='v3',
      scopes=[scope],
      key_file_location=key_file_location)
        
    results = service.files().list(q=f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'").execute()
    folders = results.get('files', [])
    folder_id = None

    # Print the folder's ID if found
    if len(folders) > 0:
      logger.info(f"Folder ID: {folders[0]['id']}")
      logger.info(f"Folder name: {folders[0]['name']}")
      folder_id = folders[0]['id']
    else:
      logger.info("Folder not found.")

    # Delete all files in the local contracts folder
    download_dir = 'contracts'
    logger.info('Deleting all files in the contracts folder...')
    for file in os.listdir(download_dir):
      file_path = os.path.join(download_dir, file)
      try:
        if os.path.isfile(file_path):
          os.unlink(file_path)
      except Exception as e:
        logger.error(f"An error occurred while deleting file: {file_path}")
        logger.error(e)
    # Getting all files in the contracts folder
    results = service.files().list(q=f"'{folder_id}' in parents and trashed=false", pageSize=1000, fields="nextPageToken, files(id, name, createdTime)").execute()
    files = results.get('files', [])
    
    logger.info('Downloading contract ABIs...')
    # Download each file from the folder
    for file in files:
      request_file = service.files().get_media(fileId=file['id'])
      # Get the file metadata
      file_metadata = service.files().get(fileId=file['id']).execute()
      file_name = file_metadata['name']
      created_time = datetime.strptime(file['createdTime'], "%Y-%m-%dT%H:%M:%S.%fZ")
      logger.info(f"File Name: {file_name}, Created Time: {created_time}")

      # Download the file content
      fh = open(os.path.join("contracts", file_name), 'wb')
      downloader = MediaIoBaseDownload(fh, request_file)

      done = False
      while done is False:
        status, done = downloader.next_chunk()
        logger.info(f"Download progress {int(status.progress() * 100)}%.")

      print('File downloaded successfully.')

    logger.info('Contract ABIs downloaded successfully!')

    # Load and parse the contract ABIs.
    with open('contracts/FootballSquares.json') as f:
      global rps_contract_abi
      rps_json = json.load(f)
      rps_contract_abi = rps_json['abi']

  except HttpError as error:
    # TODO(developer) - Handle errors from drive API.
    logger.error(f'An error occurred: {error}')

def get_gas_oracle():
  url = "https://api.etherscan.io/api"
  payload = {
    'module': 'gastracker',
    'action': 'gasoracle',
    'apikey': GAS_ORACLE_API_KEY
  }

  for _ in range(5):
    try:
      response = requests.get(url, params=payload)
      response.raise_for_status()
      return response.json()
    except (requests.exceptions.RequestException, KeyError):
      time.sleep(12)

    raise Exception("Failed to fetch gas price after several attempts")

def get_eth_price():
  for _ in range(5):
    try:
      response = requests.get(COINGECKO_API)
      response.raise_for_status()  # Raise an exception if the request was unsuccessful
      data = response.json()
      return data['ethereum']['usd']
    except (requests.exceptions.RequestException, KeyError):
      time.sleep(12)

    # If we've gotten to this point, all the retry attempts have failed
    raise Exception("Failed to fetch Ethereum price after several attempts")

def usd_to_eth(usd):
  eth_price = ethereum_prices[-1] #get_eth_price()
  return usd / eth_price

@app.route('/ethereum-price', methods=['GET'])
def handle_get_ethereum_price():
  game_id = request.args.get('game_id')
  
  game = cosmos_games_container.read_item(item=game_id, partition_key=WEEK_ID)

  if not game:
    return

  return str(ethereum_prices[-1])

@app.route('/gas-oracle', methods=['GET'])
def handle_get_gas_oracle():
  game_id = request.args.get('game_id')
  
  game = cosmos_games_container.read_item(item=game_id, partition_key=WEEK_ID)

  if not game:
    return

  return gas_oracles[-1]

def send_to_all_except(event:str, message:Any, game: Game, excluded_player_id:str):
  logger.info(f"Broadcasting message to all except: {excluded_player_id}")
  for player_id in game.players:
    if player_id != excluded_player_id:      
      room = f"{player_id}-{game.id}"
      logger.info(f"Sending message {message} to room: {room}")
      emit(event, message, to=room, namespace='/')
    
def send_to_all(event:str, message:Any, game: Game):
  logger.info(f"Broadcasting '{event}' to all players in game: {game.id}")
  for player_id in game.players:
    global redis_client
    offset = int(redis_client.get('scoring_play_offset'))
    logger.info(f"Offset: {offset}")

    room = f"{player_id}-{game.id}"

    logger.info(f"Sending '{event}' to room: {room}")

    emit(event, message, to=room, namespace='/')
    socketio.sleep(0)
      
def ack_message(response):
  logger.info(f"Acknowledgment received for '{response}")

def process_scoring_play(scoring_play: ScoringPlay):
  global cosmos_games_container
  global redis_client
  try:
    result = cosmos_games_container.read_item(item=scoring_play.game_id, partition_key=scoring_play.week_id)
    game = Game(**result)
    
    redis_client.incr('scoring_play_offset')
    scoring_play.offset = int(redis_client.get('scoring_play_offset'))
    # logger.info(f"Scoring play: {scoring_play.model_dump_json()} will be added to game: {game.id}")

    game.scoring_plays.append(scoring_play)

    # logger.info(f"Game state after adding scoring plays: {game.model_dump_json()}")
    # cosmos_games_container.replace_item(item=game.id, body=game.model_dump())
    # logger.info(f"Successfully added scoring plays to game: {game.id}")
    
    home_points_ones = scoring_play.home_points % 10
    away_points_ones = scoring_play.away_points % 10

    square_id_to_find = f"{home_points_ones}{away_points_ones}" # home = row, away = column

    logger.info(f"Square id to find: {square_id_to_find}")

    square = find_square_by_id(game.claimed_squares, square_id_to_find)

    if square is not None:
      message = {
        'event_num': scoring_play.event_num,
        'square': square.model_dump(),
        'scoring_play': scoring_play.model_dump()
      }

      logger.info(f"Square found: {square}")
      
      emit('square_match', message, to=f"{square.player_id}-{game.id}", namespace='/', callback=ack_message)
      send_to_all_except('mark_claimed_square_match', message, game, square.player_id)
    else:
      square_data = {
        "id": square_id_to_find, 
        "game_id": game.id, 
        "week_id": game.week_id, 
        "player_id":'',
        "home_points": home_points_ones,
        "away_points": away_points_ones
      }
      message = {
        'event_num': scoring_play.event_num,
        'square': Square(**square_data).model_dump(),
        'scoring_play': scoring_play.model_dump()
      }

      # logger.info(f"No square found for scoring play: {scoring_play.model_dump_json()}")
      # send_to_all('mark_unclaimed_square_match', message, game)

      with app.app_context():
        emit('mark_unclaimed_square_match', message, to=game.id, namespace='/', callback=ack_message)
        socketio.sleep(0)

      logger.info(f"Broadcasted message to all players in game: {game.id}")
  
  except CosmosResourceNotFoundError:
    logger.error(f"Game {scoring_play.id} not found.")
    # return jsonify([])

  # return jsonify({ 'success': True })

def find_square_by_id(squares: List[Square], square_id: str):
  for square in squares:
    if square.id == square_id:
      return square

def filter_square_from_game(game: Game, square: Square):
  return [s for s in game.claimed_squares if s.id != square.id]
    
def ten_min_from_start(game: Game):
  scheduled_time = datetime.strptime(game.scheduled, '%Y-%m-%dT%H:%M:%S%z')
  current_time = datetime.now(timezone.utc)
  time_difference = scheduled_time - current_time
  if time_difference.total_seconds() >= 600:
    return True

def get_scheduled_games_for_week():
  global WEEK_ID
  global cosmos_games_container
  results = list(cosmos_games_container.query_items(
    # query="SELECT * FROM c WHERE c.status=@closed and c.week_id=@week_id",
    query="SELECT * FROM c WHERE c.week_id=@week_id",

    parameters=[
      # {"name": "@closed", "value": "closed"},
      {"name": "@week_id", "value": WEEK_ID}
    ],
    enable_cross_partition_query=True
  ))

  # game1 = Game(**results[0])
  # logger.info(f"Game 1: {game1.model_dump_json()}")

  return [Game(**game) for game in results]

@app.route('/games', methods=['GET'])
def get_games():
  games = get_scheduled_games_for_week()
  # scheduled_games = [game for game in games if ten_min_from_start(game)]
  scheduled_games = [game for game in games]

  return jsonify([{
    "week_id": game.week_id,
    "game_id": game.id, 
    "name": game.name} for game in scheduled_games])

@app.route('/fs-contract-abi', methods=['GET'])
def get_fs_contract_abi():
  with open('contracts/FootballSquares.json') as f:
    return json.load(f)
  
# if the connection state recovery was not successful
@socketio.on('reconnect_failed')
def handle_reconnect_failed(data):
  logger.info('Client reconnection failed.')
  logger.info(f"Data: {data}")
  # get the scoring plays for the game where the scoring play offset is greater than the given offset
  global cosmos_games_container
  results = list(cosmos_games_container.query_items(
    query="SELECT * FROM c WHERE c.game_id!=@game_id and c.week_id=@week_id",
    parameters=[
      {"name": "@game_id", "value": data['game_id']},
      {"name": "@week_id", "value": data['week_id']}
    ],
    enable_cross_partition_query=True
  ))

@socketio.on_error(namespace='/')
def socketio_error_handler(e):
    logger.error(f"An error has occurred: {e}")

@socketio.on('leave_game')
def leave_game(data):
  global cosmos_games_container
  result = cosmos_games_container.read_item(item=data['game_id'], partition_key=data['week_id'])
  game = Game(**result)

  if data['player_id'] in game.players:
    logger.info(f"Player {data['player_id']} left game: {game.id}")

    # Get the squares where square.player_id equals data['player_id']
    unclaimed_squares = [square.model_dump() for square in game.claimed_squares if square.player_id == data['player_id']]
    remaining_squares = [square for square in game.claimed_squares if square.player_id != data['player_id']]
    
    game.claimed_squares.clear()

    game.claimed_squares = remaining_squares

    logger.info(f"Game state after unclaiming squares: {game.model_dump_json()}")

    leave_room(game.id)
    leave_room(f"{data['player_id']}-{game.id}")

    game.players.remove(data['player_id'])
    logger.info(f"Game state after removing player: {game.model_dump_json()}")

    cosmos_games_container.replace_item(item=game.id, body=game.model_dump())

    message = {
      'player_id': data['player_id'], 
      'game_id': game.id,
      'unclaimed_squares': unclaimed_squares
    }
    
    send_to_all_except('player_left_game', message, game, data['player_id'])

@socketio.on('join_game')
def join_game(data):
  global cosmos_games_container
  result = cosmos_games_container.read_item(item=data['game_id'], partition_key=data['week_id'])
  game = Game(**result)
  logger.info(f"Game state before player {data['player_id']} joined game: {game.model_dump_json()}")

  global cosmos_players_container
  result = cosmos_players_container.read_item(item=data['player_id'], partition_key=data['week_id'])
  player = Player(**result)
  logger.info(f"Player state before joining game: {player.model_dump_json()}")

  if player.id in game.players:
    logger.info(f"Player {player.id} already joined game.")
    join_room(game.id, namespace='/')
    join_room(f"{player.id}-{game.id}", namespace='/')
    emit('game_joined', game.model_dump(), to=player.id, namespace='/')
    return
  
  player.games.append(game.id)
  logger.info(f"Player {player.model_dump_json()} joined game: {game.id}")
  
  cosmos_players_container.replace_item(item=player.id, body=player.model_dump())

  game.players.append(player.id)
  cosmos_games_container.replace_item(item=game.id, body=game.model_dump())

  logger.info(f"Game state after player {player.id} joined game: {game.model_dump_json()}")

  join_room(game.id, namespace='/')
  join_room(f"{player.id}-{game.id}", namespace='/')
  
  emit('game_joined', game.model_dump(), room=player.id, namespace='/')
  send_to_all_except('new_player_joined', { "player_id": player.id, "game_id": game.id }, game, player.id)

def save_games_for_current_week():
  response = requests.get(f"{SPORTRADAR_CURRENT_WEEK_URL}?api_key={SPORTRADAR_API_KEY}")
  
  global cosmos_games_container

  if response.status_code == 200:
    data = response.json()
    sportradar_game_list = data['week']['games']
    
    global WEEK_ID
    WEEK_ID = data['week']['id']

    for game in sportradar_game_list:
      try:
        # Read item to check if it exists
        existing_game = cosmos_games_container.read_item(item=game['id'], partition_key=data['week']['id'])
        existing_game['status'] = game['status']

        logger.warning(f"Game {game['id']} already exists. Updating status")
        cosmos_games_container.replace_item(item=existing_game['id'], body=existing_game)
        continue
      except CosmosResourceNotFoundError:
        name = f"{game['home']['name']} vs {game['away']['name']}"
        scheduled = game['scheduled']
        logger.info(f"Saving game: {name} scheduled for: {scheduled}")
        cosmos_games_container.create_item(body={
          'contract_address': '',
          'id': game['id'],
          'week_id': WEEK_ID,
          'name': name,
          'scheduled': scheduled,
          'players': [],
          'claimed_squares': [],
          'payouts': [],
          'status': game['status'],
          'scoring_plays': []
        })

        logger.info(f"Game {game['id']} saved: {name}")
  else:
    logger.error(f"Request failed with status code {response.status_code}")

@socketio.on('unclaim_square')
def handle_unclaim_square(square: Square):
  square = Square(**square)
  logger.info(f"Unclaiming square: {square.model_dump_json()}")
  global cosmos_games_container
  result = cosmos_games_container.read_item(item=square.game_id, partition_key=square.week_id)
  game = Game(**result)
  game.claimed_squares.remove(square)
  
  cosmos_games_container.replace_item(item=game.id, body=game.model_dump())

  logger.info(f"Game state after square unclaimed: {game.model_dump_json()}")

  send_to_all_except('square_unclaimed', square.model_dump(), game, square.player_id)

@socketio.on('unclaim_squares')
def handle_unclaim_squares(data):
  game_id = data['game_id']
  squares_to_unclaim = data['squares_to_unclaim']

  global WEEK_ID
  global cosmos_games_container
  game = cosmos_games_container.read_item(item=game_id, partition_key=WEEK_ID)

  filtered_squares = []
  for square in squares_to_unclaim:
    logger.info(f"Unclaiming square: {square}")
    # get the square from the game
    filtered_squares = filter_square_from_game(game, square['id'])

  game.claimed_squares = filtered_squares
   
  cosmos_games_container.replace_item(item=game.id, body=game.model_dump())
  logger.info(f"Successfully updated game: {game.id}")

@socketio.on('claim_square')
def handle_claim_square(square: Square):
  square = Square(**square)
  logger.info(f"Claiming square: {square.model_dump_json()}")
  global cosmos_games_container
  result = cosmos_games_container.read_item(item=square.game_id, partition_key=square.week_id)
  game = Game(**result)
  logger.info(f"Game state before square claimed: {game.model_dump_json()}")
  game.claimed_squares.append(square)
  logger.info(f"Game state after square claimed: {game.model_dump_json()}")

  cosmos_games_container.replace_item(item=game.id, body=game.model_dump())
  logger.info(f"Successfully updated game: {game.id}")

  emit('square_claimed', square.model_dump(), include_self=False, to=game.id, namespace='/')

@socketio.on('squares_claimed')
def handle_squares_claimed(data):
  player_id = data['player_id']
  game_id = data['game_id']

  global WEEK_ID
  global cosmos_games_container
  results = cosmos_games_container.read_item(item=game_id, partition_key=WEEK_ID)

  game = Game(**results)

  for square in game.claimed_squares:
    if square.player_id == player_id:
      logger.info(f"Updating square: {square.model_dump_json()}")
      square.paid = True

  cosmos_games_container.replace_item(item=game.id, body=game.model_dump())
  logger.info(f"Successfully updated game: {game}")

@socketio.on('connect')
def handle_connect():
  try:
    logger.info('Client connected.')
    
    player_id = request.args.get('player_id')

    global cosmos_games_container
    results = list(cosmos_games_container.query_items(
      query="SELECT * FROM c WHERE c.status=@status",
      parameters=[
        {"name": "@status", "value": "closed"}
      ],
      enable_cross_partition_query=True
    ))

    global WEEK_ID
    WEEK_ID = results[0]['week_id']
  
    player_data = {
      "id": player_id,
      "address": "",
      "games": [],
      "week_id": WEEK_ID
    }

    player = Player(**player_data)

    global cosmos_players_container
    try:
      cosmos_players_container.read_item(item=player.id, partition_key=player.week_id)
    except CosmosResourceNotFoundError:
      cosmos_players_container.upsert_item(body=player.model_dump())

    logger.info(f"Player {player.id} connected.")

    join_room(player.id, namespace='/')

    emit('connected', player.model_dump(), to=player.id, namespace='/')
  except Exception as e:
    logger.error(f"An error occurred in handle_connect: {str(e)}")

def create_cosmos_games_container():
  client = CosmosClient(url=COSMOS_ENDPOINT, credential=COSMOS_KEY)
  database = client.create_database_if_not_exists(id=COSMOS_DB_NAME)
  key_path = PartitionKey(path=COSMOS_GAMES_PARTITION_KEY_PATH)

  return database.create_container_if_not_exists(
    id=COSMOS_GAMES_CONTAINER_NAME,
    partition_key=key_path,
    offer_throughput=400
  )

def poll_box_score(game:Game):
  previous_scoring_plays = []
  home_team = None
  away_team = None

  start_time = datetime.now()
  logger.info(f"Started polling the boxscore for game: {game.id} at {start_time}")
  
  count = 0
  # while data['status'] != 'closed':
  while True:
    thread_id = threading.get_ident()
    logger.info(f"Polling boxscore for game id: {game.id} with thread id: {thread_id}")
    response = requests.get(f"{SPORTRADAR_BOXSCORE_URL}/{game.id}/boxscore.json?api_key={SPORTRADAR_API_KEY}")

    if response.status_code == 200:
      data = response.json()
      scoring_plays = data['scoring_plays']
      logger.info(f"Length of scoring plays: {len(scoring_plays)}")
      home_team = f"{data['summary']['home']['market']} {data['summary']['home']['name']}"
      away_team = f"{data['summary']['away']['market']} {data['summary']['away']['name']}"
    
      if len(scoring_plays) - len(previous_scoring_plays) > 0:
        starting_idx = len(scoring_plays) - (len(scoring_plays) - len(previous_scoring_plays))
        logger.info(f"Starting index: {starting_idx}")
      
        for idx in range(starting_idx, len(scoring_plays)):
          scoring_play = ScoringPlay(
            id=scoring_plays[idx]['id'],
            type=scoring_plays[idx]['type'],
            play_type=scoring_plays[idx]['play_type'],
            home_points=scoring_plays[idx]['home_points'],
            away_points=scoring_plays[idx]['away_points'],
            home_team=home_team,
            away_team=away_team,
            week_id=game.week_id,
            event_num=count,
            game_id=game.id
          )
        
          process_scoring_play(scoring_play)

        count += 1
        
        previous_scoring_plays.clear()
        previous_scoring_plays = scoring_plays.copy()
    else:
      logger.error(f"Request failed with status code {response.status_code}")
      continue

    time.sleep(30)
        
def create_cosmos_players_container():
  client = CosmosClient(url=COSMOS_ENDPOINT, credential=COSMOS_KEY)
  database = client.create_database_if_not_exists(id=COSMOS_DB_NAME)
  key_path = PartitionKey(path=COSMOS_PLAYERS_PARTITION_KEY_PATH)

  return database.create_container_if_not_exists(
    id=COSMOS_PLAYERS_CONTAINER_NAME,
    partition_key=key_path,
    offer_throughput=400
  )

def get_eth_prices():
  while True:
    current_price = get_eth_price()
    logger.info(f"Current price of Ethereum: {current_price}")
    global ethereum_prices
    ethereum_prices.append(current_price)

    if len(ethereum_prices) > 10:
      ethereum_prices = ethereum_prices[-5:]

    time.sleep(45)

def get_gas_oracles():
  while True:
    gas_oracle = get_gas_oracle()
    logger.info(f"Gas oracle: {gas_oracle}")
    global gas_oracles
    gas_oracles.append(gas_oracle)

    if len(gas_oracles) > 10:
      gas_oracles = gas_oracles[-5:]

    time.sleep(30)

if __name__ == '__main__':
  from geventwebsocket.handler import WebSocketHandler
  from gevent.pywsgi import WSGIServer

  logger.info('Downloading contract ABIs...')
  download_contract_abi()

  cosmos_games_container = create_cosmos_games_container()
  cosmos_players_container = create_cosmos_players_container()

  # save_games_for_current_week()

  WEEK_ID = '0796e6a9-84ca-4651-9813-bc8bb391ad95'
  
  games = get_scheduled_games_for_week()
  logger.info(f"Games for current week: {games}")

  # polling_threads = []
  
  # for game in games:
  #   polling_thread = threading.Thread(target=poll_box_score, args=(game))
  #   polling_threads.append(polling_thread)
  #   polling_thread.start()

  # for thread in polling_threads:
  #   thread.join()
  
  # Write each pid from the threads array to file
  # with open('worker_pids.txt', 'a') as file:
  #   for p in polling_threads:
  #     file.write(f"{p.pid}\n")
  thread = threading.Thread(target=get_eth_prices)
  thread.start()
  
  thread = threading.Thread(target=get_gas_oracles)
  thread.start()

  logger.info('Starting server...')
  
  http_server = WSGIServer(('0.0.0.0', 443),
                           app,
                           keyfile=KEYFILE,
                           certfile=CERTFILE,
                           handler_class=WebSocketHandler)

  http_server.serve_forever()  
