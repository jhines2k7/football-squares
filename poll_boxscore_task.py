import json
import time
import random
from azure.eventhub import EventHubProducerClient, EventData
import os
import datetime
import logging
import sys
from dotenv import load_dotenv

logging.basicConfig(
  stream=sys.stderr,
  level=logging.DEBUG,
  format='%(levelname)s:%(asctime)s:%(message)s'
)

logger = logging.getLogger(__name__)

# Load the .env file specified in the command-line arguments
load_dotenv(".env.poll_boxscore")

EVENTHUB_CONNECTION_STR = os.getenv("EVENTHUB_CONNECTION_STR")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")

def send_event(event_hub_connection_str, event_hub_name, event_data):
  producer = EventHubProducerClient.from_connection_string(
    conn_str=event_hub_connection_str, 
    eventhub_name=event_hub_name)
    
  with producer:
    event_data_batch = producer.create_batch()

    event_data_batch.add(EventData(event_data))

    producer.send_batch(event_data_batch)

connection_str = EVENTHUB_CONNECTION_STR
event_hub_name = EVENTHUB_NAME

def poll_boxscore(game_id):
  start_time = datetime.datetime.now()
  logger.info(f"Started polling the boxscore for game: {game_id} at {start_time}")
  
  pid = os.getpid()
  indices = set(random.sample(range(30), 10))

  for i in range(30):
    logger.info(f"Polling boxscore for game id: {game_id} on PID: {pid}")
    time.sleep(5)

    if i in indices:
      event_data = {
        'count': i,
        'game_id': game_id,
      }

      send_event(connection_str, event_hub_name, json.dumps(event_data))
  
  logger.info(f"Finished polling boxscore for game id: {game_id}")
