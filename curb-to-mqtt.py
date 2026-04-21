#!/usr/bin/env python3
"""
Samples sink + MQTT bridge for EnergyCurb hubs (multi-device).

Accepts POST /v3/samples/<serial> on plain HTTP from any number of hubs,
decodes the deflate + MessagePack body, appends a JSON line to
samples-<serial>.jsonl, and republishes each hub's 18 per-circuit wattages
to MQTT under a per-serial namespace. Home Assistant discovery is published
once per serial, the first time that serial is seen after startup.

Usage:
    pip install msgpack paho-mqtt pyyaml
    python samples-to-mqtt.py [--config config.yaml] [--host 0.0.0.0] [--port 80]

config.yaml keys:
    MQTT_BROKER_URL         e.g. "mqtt://host:1883"
    MQTT_TOPIC              base topic, e.g. "home/curb/power"
    MQTT_USERNAME           optional
    MQTT_PASSWORD           optional
    HA_DISCOVERY_PREFIX     default "homeassistant"
    DEVICE_NAME             display-name prefix, e.g. "Curb Energy Monitor"
    DEVICES                 map of serial → { CIRCUIT_NAMES: [...] }
    DEFAULT_CIRCUIT_NAMES   optional fallback list of up to 18 names
    DEBUG                   bool
"""

import argparse
import datetime
import json
import re
import sys
import threading
import zlib
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

import msgpack
import paho.mqtt.client as mqtt
import yaml

SAMPLES_PATH_RE = re.compile(r"^/v3/samples/([^/?#]+)")
NUM_CIRCUITS    = 18
WH_PER_SEC_TO_W = 3600   # interval is 1 s, so Wh * 3600 = W


# ── config ────────────────────────────────────────────────────────────────────

class Config:
    def __init__(self, path: str):
        with open(path) as f:
            c = yaml.safe_load(f) or {}
        self.broker_url            = c.get("MQTT_BROKER_URL", "mqtt://localhost:1883")
        self.topic                 = c.get("MQTT_TOPIC", "curb/power")
        self.username              = c.get("MQTT_USERNAME") or None
        self.password              = c.get("MQTT_PASSWORD") or None
        self.ha_prefix             = c.get("HA_DISCOVERY_PREFIX", "homeassistant")
        self.device_name           = c.get("DEVICE_NAME", "Curb Energy Monitor")
        self.default_circuit_names = c.get("DEFAULT_CIRCUIT_NAMES") or c.get("CIRCUIT_NAMES") or []
        self.devices               = c.get("DEVICES") or {}
        self.debug                 = bool(c.get("DEBUG", False))

    @staticmethod
    def safe(s: str) -> str:
        return re.sub(r"[^a-z0-9_]", "_", s, flags=re.IGNORECASE)

    def device_id(self, serial: str) -> str:
        return f"curb_{self.safe(serial)}"

    def device_display_name(self, serial: str) -> str:
        return f"{self.device_name} {serial}"

    def circuit_names_for(self, serial: str) -> list:
        entry = self.devices.get(serial)
        if isinstance(entry, dict):
            names = entry.get("CIRCUIT_NAMES")
            if names:
                return names
        return self.default_circuit_names

    def circuit_name(self, serial: str, idx: int) -> str:
        names = self.circuit_names_for(serial)
        if idx < len(names) and names[idx]:
            return names[idx]
        return f"Circuit {idx + 1}"

    def circuit_object_id(self, serial: str, idx: int) -> str:
        return f"{self.device_id(serial)}_circuit_{idx + 1}"

    def state_topic(self, serial: str, idx: int) -> str:
        return f"{self.topic}/{self.safe(serial)}/circuit_{idx + 1}"

    def samples_path(self, serial: str) -> str:
        return f"samples-{self.safe(serial)}.jsonl"


def debug(cfg: Config, *args):
    if cfg.debug:
        print("[DEBUG]", *args)


# ── MQTT ──────────────────────────────────────────────────────────────────────

def connect_mqtt(cfg: Config) -> mqtt.Client:
    u = urlparse(cfg.broker_url)
    host = u.hostname or "localhost"
    port = u.port or 1883

    client = mqtt.Client()
    if cfg.username:
        client.username_pw_set(cfg.username, cfg.password or "")

    def on_connect(c, _u, _f, rc):
        if rc == 0:
            print(f"[mqtt] connected to {host}:{port}")
        else:
            print(f"[mqtt] connect failed rc={rc}")

    def on_disconnect(_c, _u, rc):
        print(f"[mqtt] disconnected rc={rc}")

    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect

    print(f"[mqtt] connecting to {host}:{port} …")
    client.connect_async(host, port, keepalive=60)
    client.loop_start()
    return client


def publish_discovery(client: mqtt.Client, cfg: Config, serial: str):
    device = {
        "identifiers":  [cfg.device_id(serial)],
        "name":         cfg.device_display_name(serial),
        "model":        "Curb",
        "manufacturer": "Curb",
    }
    for i in range(NUM_CIRCUITS):
        obj_id = cfg.circuit_object_id(serial, i)
        topic  = f"{cfg.ha_prefix}/sensor/{cfg.device_id(serial)}/{obj_id}/config"
        payload = {
            "name":                cfg.circuit_name(serial, i),
            "unique_id":           obj_id,
            "default_entity_id":   f"sensor.{obj_id}",
            "state_topic":         cfg.state_topic(serial, i),
            "value_template":      "{{ value_json.power | round(1) }}",
            "unit_of_measurement": "W",
            "device_class":        "power",
            "state_class":         "measurement",
            "device":              device,
        }
        client.publish(topic, json.dumps(payload), qos=0, retain=True)
        debug(cfg, f"discovery → {topic}")


def publish_sample(client: mqtt.Client, cfg: Config, serial: str, sample: dict) -> int:
    """Flatten a single sample's groups into 18 watt readings and publish."""
    t = sample.get("t")
    watts = []
    for group in sample.get("g", []):
        for ch in group.get("c", []):
            w_wh = ch.get("w") or 0
            watts.append(abs(w_wh) * WH_PER_SEC_TO_W)
    if len(watts) != NUM_CIRCUITS:
        debug(cfg, f"{serial}: expected {NUM_CIRCUITS} circuits, got {len(watts)}; skipping t={t}")
        return 0
    for i, w in enumerate(watts):
        client.publish(
            cfg.state_topic(serial, i),
            json.dumps({"circuit": i + 1, "power": w, "t": t}),
            qos=0, retain=True,
        )
    return len(watts)


# ── HTTP handler ──────────────────────────────────────────────────────────────

class SamplesHandler(BaseHTTPRequestHandler):
    mqtt_client: mqtt.Client = None    # set by main()
    cfg: Config = None                 # set by main()

    # Track serials for which discovery has already been published this run.
    _discovered: set = set()
    _discovery_lock = threading.Lock()

    def _ensure_discovery(self, serial: str):
        with self._discovery_lock:
            if serial in self._discovered:
                return
            self._discovered.add(serial)
            new = True
        if new:
            print(f"[mqtt] publishing HA discovery for new device: {serial}")
            publish_discovery(self.mqtt_client, self.cfg, serial)

    def do_POST(self):
        m = SAMPLES_PATH_RE.match(self.path)
        if not m:
            if not self.path.startswith("/v3"):
                print(f"[http] unexpected POST path: {self.path}  from {self.client_address[0]}")
            self.send_error(404)
            return

        serial = m.group(1)
        length = int(self.headers.get("Content-Length", 0))
        raw    = self.rfile.read(length) if length else b""

        data = raw
        if self.headers.get("Content-Encoding", "").lower() == "deflate":
            try:
                data = zlib.decompress(raw)
            except zlib.error:
                try:
                    data = zlib.decompress(raw, -zlib.MAX_WBITS)
                except zlib.error as e:
                    print(f"[samples] deflate error from {serial}: {e}")
                    self.send_error(400, "bad deflate body")
                    return

        try:
            payload = msgpack.unpackb(data, raw=False, strict_map_key=False)
        except Exception as e:
            print(f"[samples] msgpack error from {serial}: {e}")
            self.send_error(400, "bad msgpack body")
            return

        record = {
            "timestamp":   datetime.datetime.now().isoformat(timespec="milliseconds"),
            "device":      serial,
            "remote_addr": self.client_address[0],
            "payload":     payload,
        }
        if self.cfg.debug:
            samples_path = self.cfg.samples_path(serial)
            with open(samples_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        # First time we see this serial this run → publish HA discovery.
        self._ensure_discovery(serial)

        # Republish each sample to MQTT (oldest first → retained value is newest).
        samples = payload.get("s", []) if isinstance(payload, dict) else []
        samples = sorted(samples, key=lambda s: s.get("t", 0))
        published = 0
        for s in samples:
            published += 1 if publish_sample(self.mqtt_client, self.cfg, serial, s) else 0

        if self.cfg.debug:
            print(f"[samples] {record['timestamp']}  {serial}  {len(samples)} sample(s), "
                f"{published} published → {samples_path}")

        body = b'{"messages":0}'
        self.send_response(200)
        self.send_header("Content-Type",   "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):    self.send_error(404)
    def do_PUT(self):    self.send_error(404)
    def do_PATCH(self):  self.send_error(404)
    def do_DELETE(self): self.send_error(404)
    def do_HEAD(self):   self.send_error(404)
    def do_OPTIONS(self): self.send_error(404)

    def log_message(self, *_):
        pass


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="EnergyCurb samples → MQTT bridge (multi-device)")
    ap.add_argument("--config", default="config.yaml", help="Config YAML path (default: config.yaml)")
    ap.add_argument("--host",   default="0.0.0.0",    help="HTTP bind address (default: 0.0.0.0)")
    ap.add_argument("--port",   default=80, type=int, help="HTTP listen port  (default: 80)")
    args = ap.parse_args()

    try:
        cfg = Config(args.config)
    except FileNotFoundError:
        print(f"[fatal] config file not found: {args.config}", file=sys.stderr)
        sys.exit(1)

    client = connect_mqtt(cfg)

    SamplesHandler.mqtt_client = client
    SamplesHandler.cfg         = cfg

    server = HTTPServer((args.host, args.port), SamplesHandler)
    print(f"[server] Listening on http://{args.host}:{args.port}/v3/samples/<serial>")
    print(f"[server] Samples log : samples-<serial>.jsonl  (one file per device)")
    print(f"[server] MQTT topic  : {cfg.topic}/<serial>/circuit_1..{NUM_CIRCUITS}")
    print(f"[server] Press Ctrl+C to stop.\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[server] Shutting down.")
        server.server_close()
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
