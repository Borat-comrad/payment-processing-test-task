"""Run inside the Compose network: python tests/smoke.py."""

import json
import os
import queue
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import httpx


def main():
    notifications = queue.Queue()

    class Receiver(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            notifications.put(json.loads(body))
            self.send_response(204)
            self.end_headers()

        def log_message(self, *args):
            pass

    receiver = ThreadingHTTPServer(("0.0.0.0", 0), Receiver)
    thread = threading.Thread(target=receiver.serve_forever, daemon=True)
    thread.start()
    address = socket.gethostbyname(socket.gethostname())
    webhook = f"http://{address}:{receiver.server_port}/payments"
    try:
        with httpx.Client(
            base_url=os.getenv("SMOKE_API_URL", "http://api:8000"),
            headers={"X-API-Key": os.environ["API_KEY"]},
            timeout=10,
            trust_env=False,
        ) as client:
            body = {
                "amount": "199.90",
                "currency": "RUB",
                "description": "Compose smoke test",
                "metadata": {"check": "real-http"},
                "webhook_url": webhook,
            }
            headers = {"Idempotency-Key": "smoke-" + uuid4().hex}
            created = client.post("/api/v1/payments", json=body, headers=headers)
            assert created.status_code == 202, created.text
            payment_id = created.json()["payment_id"]
            notification = notifications.get(timeout=30)
            assert notification["payment_id"] == payment_id
            assert notification["status"] in {"succeeded", "failed"}
            assert notification["amount"] == "199.90"
            detail = client.get(f"/api/v1/payments/{payment_id}")
            assert detail.status_code == 200, detail.text
            assert detail.json()["status"] == notification["status"]
            duplicate = client.post("/api/v1/payments", json=body, headers=headers)
            assert duplicate.status_code == 202, duplicate.text
            assert duplicate.json()["payment_id"] == payment_id
            conflict = client.post(
                "/api/v1/payments", json={**body, "amount": "200.00"}, headers=headers
            )
            assert conflict.status_code == 409, conflict.text
            print(f"PASS: API -> outbox -> RabbitMQ -> gateway -> HTTP webhook ({payment_id})")
    finally:
        receiver.shutdown()
        receiver.server_close()
        thread.join(timeout=2)


if __name__ == "__main__":
    main()
