import logger
import json
from json import dumps
from kafka import KafkaProducer
import tokenUtils
import requests

ACCESS_TOKEN = "access_token"
REFRESH_TOKEN = "refresh_token"
TOKEN_FILE = "./token.json"

log = logger.setup_logger("refresh_token")

def refresh_bot_token(token_dict):
    log.debug("---------------")
    log.debug(token_dict)
    log.debug("---------------")

    url = 'https://slack.com//api/oauth.v2.access'
    headers = {"Content-type" : "application/x-www-form-urlencoded"}
    body = {
        "client_id": "30747277668.8197721338480",
        "client_secret":"4d14328e252b2b9f1fe21fd30e816cc7",
        "grant_type":"refresh_token",
        # "refresh_token":token_dict[REFRESH_TOKEN]
        "refresh_token":token_dict["refresh_token"]
    }

    response = requests.post(url, headers=headers, data=body).text
    response = json.loads(response)
    log.debug("SLACK RESPONSE")
    log.debug(response)

    if (response["ok"] == True):
        token_dict[REFRESH_TOKEN]=response[REFRESH_TOKEN]
        token_dict[ACCESS_TOKEN]=response[ACCESS_TOKEN]
    else:
        error_message = response["error"]
        log.error(f"Cound not refresh token: {error_message}")

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
        producer.send(str(tokenUtils.TOPIC_NAME), value=message)
        log.info(f"MESSAGE {message} sent to {tokenUtils.TOPIC_NAME}")
    except Exception as e:
        log.error(f"MESSAGE {message} not sent to {tokenUtils.TOPIC_NAME}")
        log.error(e)

def main():
    token_dict = tokenUtils.load_stored_token()
    token_dict = refresh_bot_token(token_dict)
    tokenUtils.save_tokens(token_dict)
    publish_message(token_dict)

main()
