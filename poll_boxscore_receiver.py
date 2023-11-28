from azure.eventhub import EventHubConsumerClient
import requests
import json
import logging
import sys
from dotenv import load_dotenv
import os

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
  logger.info(f"Scoring plays sent: {len(scoring_play_data['scoring_plays'])}")
  
  game_id = scoring_play_data["game_id"]

  url = f"https://fs.generalsolutions43.com/scoring-play/{game_id}"

  response = requests.post(url, json=scoring_play_data)

  logger.info(f"Response status code: {response.status_code}")
  logger.info(f"Response: {response.json()}")

def receive_events():
  consumer = EventHubConsumerClient.from_connection_string(
    conn_str=EVENTHUB_CONNECTION_STR, 
    eventhub_name=EVENTHUB_NAME, 
    consumer_group="$Default")
    
  with consumer:
    consumer.receive(on_event=on_event, starting_position="-1")

receive_events()