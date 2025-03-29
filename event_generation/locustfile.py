# This script simulates user event traffic for testing the streaming pipeline.
# You can run it locally with `locust -f event_generation/locustfile.py` and input your Cloud Function or API endpoint.

from locust import HttpUser, task, between
import json
import random
import time

class EventGenerator(HttpUser):
    wait_time = between(1, 2)

    @task
    def send_event(self):
        event = {
            "user_id": f"user_{random.randint(1, 100)}",
            "event_type": random.choice(["click", "scroll", "purchase"]),
            "device": random.choice(["mobile", "desktop", "tablet"]),
            "category": random.choice(["home", "product", "checkout"]),
            "region": random.choice(["th", "us", "jp"]),
            "timestamp": time.time()
        }
        headers = {"Content-Type": "application/json"}
        self.client.post("/", data=json.dumps(event), headers=headers)