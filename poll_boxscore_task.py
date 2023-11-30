import json
import time
import random
from azure.eventhub import EventHubProducerClient, EventData
import os
import datetime
import logging
import sys
from dotenv import load_dotenv
import requests
from pydantic import BaseModel
from typing import List
from models.football_squares import ScoringPlay, ScoringPlayDTO

logging.basicConfig(
  stream=sys.stderr,
  level=logging.INFO,
  format='%(levelname)s:%(asctime)s:%(message)s'
)

logger = logging.getLogger(__name__)

# Load the .env file specified in the command-line arguments
load_dotenv(".env.poll_boxscore")

EVENTHUB_CONNECTION_STR = os.getenv("EVENTHUB_CONNECTION_STR")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")
SPORTRADAR_URL = os.getenv("SPORTRADAR_URL")
SPORTRADAR_API_KEY = os.getenv("SPORTRADAR_API_KEY")

def send_event(event_data):
  producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENTHUB_CONNECTION_STR, 
    eventhub_name=EVENTHUB_NAME)
    
  with producer:
    event_data_batch = producer.create_batch()

    event_data_batch.add(EventData(event_data))

    producer.send_batch(event_data_batch)

def poll_boxscore(game_id, week_id):
  current_scoring_plays = []
  previous_scoring_plays = []
  home_team = None
  away_team = None

  mock_game_id = "0b230e93-217b-4938-a9cf-324d95b4c8f7"
  start_time = datetime.datetime.now()
  logger.info(f"Started polling the boxscore for game: {game_id} at {start_time}")

  response = requests.get(f"{SPORTRADAR_URL}/{mock_game_id}/boxscore.json?api_key={SPORTRADAR_API_KEY}")

  if response.status_code == 200:
    data = response.json()
    
    home_team = f"{data['summary']['home']['market']} {data['summary']['home']['name']}"
    away_team = f"{data['summary']['away']['market']} {data['summary']['away']['name']}"

  else:
    logger.error(f"Request failed with status code {response.status_code}")
  
  pid = os.getpid()

  count = 0
  game_status = None
  # while data['status'] != 'closed':
  while game_status != 'closed':
    logger.info(f"Polling boxscore for game id: {game_id} on PID: {pid}")
    random_num = random.randint(1, 100)   
    if random_num % 5 == 0:
       # response = requests.get(f"{SPORTRADAR_URL}/{game_id}/boxscore.json?api_key={SPORTRADAR_API_KEY}")
      random_idx = random.randint(0, len(data['scoring_plays']) - 1)
      current_scoring_plays.append(
        ScoringPlay(
          id=data['scoring_plays'][random_idx]['id'],
          type=data['scoring_plays'][random_idx]['type'],
          play_type=data['scoring_plays'][random_idx]['play_type'],
          home_points=data['scoring_plays'][random_idx]['home_points'],
          away_points=data['scoring_plays'][random_idx]['away_points'],
          home_team=home_team,
          away_team=away_team
        )
      )       

      set_current_scoring_play_ids = set(scoring_play.id for scoring_play in current_scoring_plays)
      set_previous_scoring_play_ids = set(scoring_play.id for scoring_play in previous_scoring_plays)
      diff_ids = set_current_scoring_play_ids - set_previous_scoring_play_ids
      new_scoring_plays = [scoring_play for scoring_play in current_scoring_plays if scoring_play.id in diff_ids]

      previous_scoring_plays = current_scoring_plays.copy()

      if len(new_scoring_plays) > 0:
        logger.info(f"Found new scoring plays: {new_scoring_plays}")
        scoring_play_dto = ScoringPlayDTO(game_id=game_id, week_id=week_id, scoring_plays=new_scoring_plays)
        logger.info(f"Sending event dto: {scoring_play_dto.model_dump_json()}")
        send_event(scoring_play_dto.model_dump_json())

    time.sleep(5)

    count += 1

    if count == 1200:
      game_status = 'closed'

    # game_status = data['status']
  
  logger.info(f"Finished polling boxscore for game id: {game_id}")
