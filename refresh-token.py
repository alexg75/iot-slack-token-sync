import logger
import json
from json import dumps
from kafka import KafkaProducer

log = logger.setup_logger("refresh_token")

ACCESS_TOKEN = "access_token"
REFRESH_TOKEN = "refresh_token"
TOKEN_FILE = "/token.json"
TOPIC_NAME = 'slack-token'

def load_stored_token():
    token_dict = {}
    try:        
        json_file = open(TOKEN_FILE, "r")        
        token_dict = json.load(json_file)
        log.info(f"TOKEN LOADED: {token_dict}")
    except Exception as e:
        log.error("Could not load stored token")
        log.error(e)
    return token_dict

def refresh_bot_token(token_dict):
    # TODO might need to create a new slack appfor testing
    return token_dict

def save_tokens(token_dict):
    try:
        with open(TOKEN_FILE, "w") as outfile:
            json.dump(token_dict, outfile)
    except:
        log.error("Could not save token")

def publish_message(message):
    try:
        producer = KafkaProducer(bootstrap_servers=['rp-queue2:29092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
        producer.send(str(TOPIC_NAME), value=message)
        log.info(f"MESSAGE {message} sent to {TOPIC_NAME}")
    except Exception as e:
        log.error(f"MESSAGE {message} not sent to {TOPIC_NAME}")
        log.error(e)

def main():
    token_dict = load_stored_token()
    token_dict = refresh_bot_token(token_dict)
    save_tokens(token_dict)
    publish_message(token_dict)

main()
