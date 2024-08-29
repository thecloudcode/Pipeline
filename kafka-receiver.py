import json
from kafka import KafkaConsumer
import config

kafka_server = [config.server]

topics = [
    "campaign_performance_topic",
    "leads_generated_topic",
    "phone_metrics_topic",
    "tokens_paid_topic",
    "candidate_application_tracker_topic",
    "webinar_leads_topic"
]

consumer = KafkaConsumer(
    *topics,
    bootstrap_servers = kafka_server,
    value_deserializer = lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset = "latest",
)

while True:
    for message in consumer:
        print(f"Received message from topics {message.topic}")
        print(message.value)
















# kafka_server = [config.server]
# topic = "test_topic"
#
# consumer = KafkaConsumer(
#     bootstrap_servers = kafka_server,
#     value_deserializer = json.loads,
#     auto_offset_reset = "latest",
# )
#
# consumer.subscribe(topic)
#
# while True:
#     data =next(consumer)
#     print(data)
#     print(data.value)