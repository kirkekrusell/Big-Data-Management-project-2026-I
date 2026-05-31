import os
import requests


def send_slack_message(text: str):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        print("Slack webhook not configured")
        return

    response = requests.post(
        webhook_url,
        json={"text": text},
        timeout=10,
    )

    print(
        f"Slack response: {response.status_code}"
    )