"""Prometheus metrics HTTP server (stdlib only)."""
from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

from agents.crawler.platform.metrics import format_prometheus, get_crawler_metrics


class _MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        path = self.path.split("?", 1)[0]
        if path in ("/metrics", "/"):
            body = format_prometheus(get_crawler_metrics()).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return
        self.send_error(404)

    def log_message(self, format: str, *args) -> None:
        pass


def serve_metrics(port: int = 9090, *, host: str = "0.0.0.0") -> None:
    server = HTTPServer((host, port), _MetricsHandler)
    print(f"Crawler metrics on http://{host}:{port}/metrics")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nMetrics server stopped.")


def serve_metrics_background(port: int = 9090, *, host: str = "0.0.0.0") -> threading.Thread:
    thread = threading.Thread(target=serve_metrics, args=(port,), kwargs={"host": host}, daemon=True)
    thread.start()
    return thread
