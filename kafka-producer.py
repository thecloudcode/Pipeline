import logging
import requests
import json
from time import sleep
from datetime import datetime
from kafka import KafkaProducer
import config

kafka_server = [config.server]
topic_campaign = "campaign_performance_topic"
topic_leads = "leads_generated_topic"
topic_phone = "phone_metrics_topic"
topic_tokens = "tokens_paid_topic"
topic_candidate = "candidate_application_tracker_topic"
topic_webinar = "webinar_leads_topic"

producer = KafkaProducer(
    bootstrap_servers=kafka_server,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def sanitize_data(data):
    for key, value in data.items():
        if isinstance(value, float) and (value != value or value in [float('inf'), float('-inf')]):
            data[key] = None
    return data


def fetch_and_send(api_url, topic):
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()

        sanitized_data = sanitize_data(data)
        print(f"Sending sanitized data to topic {topic}: {sanitized_data}")

        producer.send(topic, sanitized_data)
        producer.flush()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"General error occurred: {req_err}")
    except json.JSONDecodeError as json_err:
        print(f"JSON decode error: {json_err}")
        print(f"Response content: {response.content}")
    except ValueError as val_err:
        print(f"Value error: {val_err}")


def main():
    logging.info("START")
    for i in range(170000):
        fetch_and_send("http://localhost:8000/campaignperformance", topic_campaign)
        fetch_and_send("http://localhost:8000/leadsgenerated", topic_leads)
        fetch_and_send("http://localhost:8000/phonemetrics", topic_phone)
        fetch_and_send("http://localhost:8000/tokenspaid", topic_tokens)
        fetch_and_send("http://localhost:8000/candidateapplicationtracker", topic_candidate)
        fetch_and_send("http://localhost:8000/webinars", topic_webinar)

        sleep(3)
    logging.info("COMPLETED")


main()
