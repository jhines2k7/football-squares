from azure.eventhub import EventHubConsumerClient
import requests
import json
import logging
import sys
from dotenv import load_dotenv
import os
from models import ScoringPlayDTO

load_dotenv(".env.poll_boxscore")

EVENTHUB_CONNECTION_STR = os.getenv("EVENTHUB_CONNECTION_STR")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")

logging.basicConfig(
  stream=sys.stderr,
  level=logging.INFO,
  format='%(levelname)s:%(asctime)s:%(message)s'
)

logger = logging.getLogger(__name__)

def on_event(partition_context, event):
  logger.info("Received event from partition: {}".format(partition_context.partition_id))
  scoring_play_data = json.loads(event.body_as_str())
  logger.info(f"Scoring play data from event: {scoring_play_data}")

  try:
    if scoring_play_data["scoring_plays"]:
      scoring_play_data["scoring_play"] = scoring_play_data["scoring_plays"][0]
      del scoring_play_data["scoring_plays"]
  except KeyError:
    logger.error("Missing key 'scoring_plays' in scoring_play_data")

    scoring_play_dto = ScoringPlayDTO(**scoring_play_data)
    
    game_id = scoring_play_dto.game_id

    url = f"https://fs.generalsolutions43.com/scoring-play/{game_id}"

    response = requests.post(url, json=scoring_play_dto.model_dump_json())

    logger.info(f"Response status code: {response.status_code}")
    logger.info(f"Response: {response.json()}")

    partition_context.update_checkpoint(event)

def receive_events():
  consumer = EventHubConsumerClient.from_connection_string(
    conn_str=EVENTHUB_CONNECTION_STR, 
    eventhub_name=EVENTHUB_NAME, 
    consumer_group="$Default")
    
  with consumer:
    consumer.receive(on_event=on_event, starting_position="-1")

receive_events()