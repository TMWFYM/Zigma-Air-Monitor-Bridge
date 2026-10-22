#!/usr/bin/env python3
"""
Zigma Aerio 300 <-> Home Assistant MQTT Bridge
Bi-directional real-time telemetry streaming and control over MQTT.
Supports Home Assistant Add-on (/data/options.json), Docker, and standalone execution.
Supports both static JWT token and automated username/password login.
Auto-discovers all purifiers on account and persists device ordering across restarts.
Periodically fetches live outdoor air quality from Grit Cloud weather service.
"""

import base64
import hashlib
import json
import logging
import os
import ssl
import sys
import time
import threading
import paho.mqtt.client as mqtt
import requests
import websocket

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("zigma-bridge")

# 1. Load Home Assistant Add-on options if available
ha_options = {}
if os.path.exists("/data/options.json"):
    try:
        with open("/data/options.json", "r") as f:
            ha_options = json.load(f)
        logger.info("Loaded configuration from Home Assistant Add-on options (/data/options.json)")
    except Exception as e:
        logger.warning("Failed to load /data/options.json: %s", e)


def get_config(key, env_var, default=""):
    val = ha_options.get(key)
    if val is not None and str(val).strip():
        return str(val).strip()
    return os.getenv(env_var, default).strip()


def get_config_int(key, env_var, default):
    val = ha_options.get(key)
    if val is not None and str(val).strip():
        try:
            return int(val)
        except ValueError:
            pass
    return int(os.getenv(env_var, default))


# MQTT Configuration
MQTT_HOST = get_config("mqtt_host", "MQTT_HOST", "core-mosquitto")
MQTT_PORT = get_config_int("mqtt_port", "MQTT_PORT", 1883)
MQTT_USER = get_config("mqtt_user", "MQTT_USER", "")
MQTT_PASSWORD = get_config("mqtt_password", "MQTT_PASSWORD", "")
MQTT_TOPIC_PREFIX = get_config("mqtt_topic_prefix", "MQTT_TOPIC_PREFIX", "zigma/aerio-300")
HA_DISCOVERY_PREFIX = get_config("ha_discovery_prefix", "HA_DISCOVERY_PREFIX", "homeassistant")

# Zigma Purifier Configuration
THING_NAME = get_config("thing_name", "THING_NAME", "")
SUB_TYPE = get_config("sub_type", "SUB_TYPE", "aerio-300")
REGION = get_config("region", "REGION", "ap-southeast-1")
SYNC_INTERVAL = get_config_int("sync_interval", "SYNC_INTERVAL", 15)

# Credentials / Token Configuration
ZIGMA_ACCOUNT = get_config("account", "ZIGMA_ACCOUNT", "")
ZIGMA_PASSWORD = get_config("password", "ZIGMA_PASSWORD", "")
ZIGMA_CALLING_CODE = get_config("calling_code", "ZIGMA_CALLING_CODE", "001")
JWT_TOKEN = get_config("jwt_token", "JWT_TOKEN", "")

# Endpoints
OAUTH_URL = get_config("oauth_url", "OAUTH_URL", "https://zigma.grit-cloud.com/prod/oauth")
API_URL = get_config("api_url", "API_URL", "https://zigma.grit-cloud.com/prod/api")
WSS_URL = get_config("wss_url", "WSS_URL", "wss://zigma.grit-cloud.com/prod/wss")

# Persistent device order mapping path
DEVICE_MAP_PATH = (
    "/data/device_map.json"
    if os.path.isdir("/data") and os.access("/data", os.W_OK)
    else os.path.join(os.path.dirname(os.path.abspath(__file__)), "device_map.json")
)


class TokenManager:
    """Manages JWT tokens thread-safely: checks validity and falls back to username/password login."""

    def __init__(self, static_token="", account="", password="", calling_code="001"):
        self.static_token = static_token.strip()
        self.account = account.strip()
        self.password = password.strip()
        self.calling_code = calling_code.strip()
        self.active_token = self.static_token
        self.token_expiry = 0
        self.lock = threading.Lock()

        if self.static_token:
            self._parse_token_expiry(self.static_token)

    def _parse_token_expiry(self, token: str):
        try:
            parts = token.split(".")
            if len(parts) == 3:
                payload_b64 = parts[1] + "=="
                payload = json.loads(base64.b64decode(payload_b64))
                self.token_expiry = payload.get("exp", 0)
        except Exception:
            self.token_expiry = 0

    def is_token_valid(self, token: str = None, min_ttl=300) -> bool:
        target_token = token if token is not None else self.active_token
        if not target_token:
            return False
        if self.token_expiry > 0:
            remaining = self.token_expiry - time.time()
            return remaining > min_ttl
        return True

    def invalidate_token(self, failed_token: str):
        with self.lock:
            if self.account and self.password and self.active_token == failed_token:
                self.token_expiry = 0
                self.active_token = ""

    def login_with_credentials(self) -> str:
        if not self.account or not self.password:
            logger.warning("No username/password provided for auto-login.")
            return ""

        logger.info("Attempting login to Grit Cloud with account %s...", self.account)
        pwd_hash = (
            hashlib.md5(self.password.encode("utf-8")).hexdigest()
            if len(self.password) != 32
            else self.password
        )
        body = {
            "header": {
                "app_version": "2.1.2",
                "app_name": "Zigma",
                "calling_code": self.calling_code,
                "account": self.account,
                "language": "en",
                "client_id": "yugong_app",
            },
            "payload": {
                "opt": "login",
                "pwd": pwd_hash,
            },
        }
        try:
            r = requests.post(OAUTH_URL, json=body, timeout=10)
            data = r.json()
            if data.get("code") == 1 and "data" in data:
                token = data["data"].get("jwt_token")
                if token:
                    logger.info("Login successful! Acquired fresh JWT token.")
                    self.active_token = token
                    self._parse_token_expiry(token)
                    return token
            logger.warning("Login failed with response: %s", data)
        except Exception as e:
            logger.error("Login request failed: %s", e)

        return ""

    def get_token(self) -> str:
        with self.lock:
            # 1. If static token is provided and still valid, use it
            if self.static_token and self.is_token_valid(self.static_token):
                return self.static_token

            # 2. If active token is valid, use it
            if self.active_token and self.is_token_valid(self.active_token):
                return self.active_token

            # 3. Token missing or expired: log in with credentials
            if self.account and self.password:
                logger.info("Static JWT token missing or expired. Authenticating with account credentials...")
                token = self.login_with_credentials()
                if token:
                    return token

            # 4. Fallback to static token even if validity cannot be guaranteed
            if self.static_token:
                logger.info("Using configured static JWT token as fallback.")
                self.active_token = self.static_token
                return self.static_token

            logger.error("No valid JWT token or credentials available!")
            return ""


def auto_discover_things(token: str, region: str) -> list:
    headers = {
        "token": token,
        "region": region,
        "content-type": "application/json; charset=utf-8",
    }
    try:
        r = requests.post(API_URL, headers=headers, json={"opt": "user_thing_list_get"}, timeout=10)
        data = r.json()
        if data.get("code") == 1 and "data" in data:
            return data["data"].get("thing_list", [])
        logger.warning("Device discovery API response: %s", data)
    except Exception as e:
        logger.error("Failed to auto-discover devices: %s", e)
    return []


# Shared weather cache across devices at the same coordinates
_weather_cache_lock = threading.Lock()
_weather_cache = {}  # (lat_round, lon_round) -> (timestamp, pm2p5)


def get_outdoor_weather(token: str, lat: float, lon: float, region: str = "ap-southeast-1") -> dict:
    key = (round(lat, 2), round(lon, 2))
    now = time.time()
    with _weather_cache_lock:
        cached = _weather_cache.get(key)
        if cached and (now - cached[0] < 600):
            return {"pm2p5": cached[1]}

    headers = {
        "token": token,
        "region": region,
        "content-type": "application/json; charset=utf-8",
    }
    body = {
        "opt": "get_weather",
        "latitude": lat,
        "longitude": lon,
        "temp_unit": "c",
    }
    try:
        r = requests.post(API_URL, headers=headers, json=body, timeout=5)
        data = r.json()
        if data.get("code") == 1 and "data" in data:
            w = data["data"]
            if w and "pm2p5" in w:
                with _weather_cache_lock:
                    _weather_cache[key] = (now, w["pm2p5"])
                logger.info("Live Outdoor PM2.5 updated from weather service: %s µg/m³", w["pm2p5"])
            return w
    except Exception as e:
        logger.debug("Failed to query Grit Cloud weather API: %s", e)
    return {}


def load_device_map() -> dict:
    """Load persistent thing_name -> metadata mapping from disk."""
    if os.path.exists(DEVICE_MAP_PATH):
        try:
            with open(DEVICE_MAP_PATH, "r") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                normalized = {}
                for k, v in raw.items():
                    if isinstance(v, int):
                        normalized[k] = {"index": v}
                    elif isinstance(v, dict) and isinstance(v.get("index"), int):
                        normalized[k] = v
                return normalized
        except Exception as e:
            logger.warning("Failed to load device map from %s: %s", DEVICE_MAP_PATH, e)
    return {}


def save_device_map(device_map: dict):
    """Persist thing_name -> metadata mapping to disk so restarts never swap HA objects."""
    try:
        with open(DEVICE_MAP_PATH, "w") as f:
            json.dump(device_map, f, indent=2, sort_keys=True)
    except Exception as e:
        logger.warning("Could not write device map to %s: %s", DEVICE_MAP_PATH, e)


def resolve_devices(discovered_devices: list, configured_thing_names: list) -> list:
    """
    Build an ordered list of device dicts with stable 1-based `index` assignments.
    Guarantees that across restarts (even if cloud API returns devices in a different order),
    each physical purifier stays permanently assigned to its Home Assistant device & entities.
    """
    device_map = load_device_map()

    # Filter discovered devices to Aerio purifiers if present, else all discovered devices
    if discovered_devices:
        matched = [
            d for d in discovered_devices
            if "aerio" in d.get("sub_type", "").lower() or "aerio" in d.get("thing_name", "").lower()
        ]
        candidate_list = matched if matched else discovered_devices
    else:
        candidate_list = []

    by_name = {}
    for d in candidate_list:
        t_name = d.get("thing_name")
        if not t_name:
            continue
        coords = d.get("lng_lat") or {}
        lat, lon = None, None
        try:
            if coords.get("latitude") is not None and coords.get("longitude") is not None:
                lat = float(coords.get("latitude"))
                lon = float(coords.get("longitude"))
        except (TypeError, ValueError):
            pass

        by_name[t_name] = {
            "thing_name": t_name,
            "sub_type": d.get("sub_type") or SUB_TYPE or "aerio-300",
            "nickname": d.get("thing_nickname") or "Air Purifier",
            "model": d.get("model") or "Aerio 300",
            "lat": lat,
            "lon": lon,
        }

    # Ensure any explicitly configured thing_name(s) are also present
    for t_name in configured_thing_names:
        if t_name not in by_name:
            saved = device_map.get(t_name, {})
            by_name[t_name] = {
                "thing_name": t_name,
                "sub_type": saved.get("sub_type") or SUB_TYPE or "aerio-300",
                "nickname": saved.get("nickname") or "Air Purifier",
                "model": saved.get("model") or "Aerio 300",
                "lat": saved.get("lat"),
                "lon": saved.get("lon"),
            }

    # If discovery returned nothing and no THING_NAME was configured, fall back to saved device_map
    if not by_name and device_map:
        logger.warning("Cloud discovery returned no devices; falling back to saved device map (%s).", DEVICE_MAP_PATH)
        for t_name, saved in device_map.items():
            by_name[t_name] = {
                "thing_name": t_name,
                "sub_type": saved.get("sub_type") or SUB_TYPE or "aerio-300",
                "nickname": saved.get("nickname") or "Air Purifier",
                "model": saved.get("model") or "Aerio 300",
                "lat": saved.get("lat"),
                "lon": saved.get("lon"),
            }

    if not by_name:
        return []

    # Share fallback coordinates across devices on the same account if one device lacks coordinates
    fallback_lat, fallback_lon = None, None
    for info in by_name.values():
        if info["lat"] is not None and info["lon"] is not None:
            fallback_lat, fallback_lon = info["lat"], info["lon"]
            break

    used_indices = set()
    for t_name, saved in device_map.items():
        idx = saved.get("index")
        if isinstance(idx, int) and idx >= 1:
            used_indices.add(idx)

    # Determine order for any newly seen devices not yet in device_map:
    # 1. Explicitly configured thing_name(s) in configured order
    # 2. Remaining devices sorted deterministically by thing_name (hardware ID)
    unmapped_names = [t for t in by_name if t not in device_map]
    unmapped_ordered = []
    for t in configured_thing_names:
        if t in unmapped_names and t not in unmapped_ordered:
            unmapped_ordered.append(t)
    for t in sorted(unmapped_names):
        if t not in unmapped_ordered:
            unmapped_ordered.append(t)

    next_idx = 1
    for t_name in unmapped_ordered:
        while next_idx in used_indices:
            next_idx += 1
        device_map[t_name] = {"index": next_idx}
        used_indices.add(next_idx)
        next_idx += 1

    # Sync metadata into device_map and build final resolved list
    resolved = []
    for t_name, info in by_name.items():
        idx = device_map[t_name]["index"]
        if info["lat"] is None and fallback_lat is not None:
            info["lat"] = fallback_lat
            info["lon"] = fallback_lon
        info["index"] = idx
        device_map[t_name] = {
            "index": idx,
            "sub_type": info["sub_type"],
            "nickname": info["nickname"],
            "model": info["model"],
            "lat": info["lat"],
            "lon": info["lon"],
        }
        resolved.append(info)

    save_device_map(device_map)
    resolved.sort(key=lambda d: d["index"])
    return resolved


class ZigmaClient:
    def __init__(
        self,
        thing_name,
        sub_type,
        on_state_update,
        token_manager: TokenManager,
        device_index=1,
        ha_device_name="Zigma Air Purifier",
        lat=None,
        lon=None,
    ):
        self.thing_name = thing_name
        self.sub_type = sub_type
        self.on_state_update = on_state_update
        self.token_manager = token_manager
        self.device_index = device_index
        self.ha_device_name = ha_device_name
        self.lat = lat
        self.lon = lon
        self.ws = None
        self.running = False
        self.lock = threading.Lock()
        self.current_state = {}
        self.last_weather_check = 0
        self.cached_outdoor_pm25 = None

    def connect(self):
        token = self.token_manager.get_token()
        if not token:
            logger.error("[Device #%d | %s] Cannot connect: No JWT token available.", self.device_index, self.thing_name)
            return False

        headers = {"token": token, "region": REGION}
        try:
            logger.info(
                "[Device #%d | %s] Connecting to Zigma Cloud WebSocket (%s)...",
                self.device_index,
                self.thing_name,
                WSS_URL,
            )
            self.ws = websocket.create_connection(
                WSS_URL,
                header=headers,
                sslopt={"cert_reqs": ssl.CERT_NONE},
                timeout=10,
            )
            logger.info("[Device #%d | %s] Connected to Zigma Cloud WebSocket.", self.device_index, self.thing_name)
            self.sync_status()
            return True
        except Exception as e:
            logger.error("[Device #%d | %s] Failed to connect to WebSocket: %s", self.device_index, self.thing_name, e)
            self.ws = None
            self.token_manager.invalidate_token(token)
            return False

    def sync_status(self):
        with self.lock:
            if not self.ws:
                return
            try:
                sync_msg = {
                    "opt": "sync_thing",
                    "sub_type": self.sub_type,
                    "thing_name": self.thing_name,
                }
                self.ws.send(json.dumps(sync_msg))
                res = self.ws.recv()
                data = json.loads(res)
                status = data.get("thing_status")
                if status:
                    # Refresh outdoor PM2.5 from weather service every 10 minutes if coordinates are available
                    now = time.time()
                    if self.lat is not None and self.lon is not None and (now - self.last_weather_check > 600 or self.cached_outdoor_pm25 is None):
                        try:
                            token = self.token_manager.get_token()
                            w = get_outdoor_weather(token, self.lat, self.lon, REGION)
                            if w and "pm2p5" in w:
                                self.cached_outdoor_pm25 = w["pm2p5"]
                            self.last_weather_check = now
                        except Exception as ex:
                            logger.debug("Weather query skipped: %s", ex)

                    if self.cached_outdoor_pm25 is not None:
                        status["outdoor_pm25"] = self.cached_outdoor_pm25

                    self.current_state = status
                    self.on_state_update(status)
            except Exception as e:
                logger.error("[Device #%d | %s] Error during sync_status: %s", self.device_index, self.thing_name, e)
                self.close()

    def send_command(self, updates: dict):
        with self.lock:
            if not self.ws and not self.connect():
                logger.error("[Device #%d | %s] Cannot send command: WebSocket disconnected.", self.device_index, self.thing_name)
                return False

            cmd_payload = {
                "state": {
                    "cmd_mode": "manual",
                    "cmd_origin": "app",
                    **updates,
                }
            }
            cmd = {
                "opt": "send_to_device",
                "sub_type": self.sub_type,
                "thing_name": self.thing_name,
                "topic_name": f"$aws/things/{self.thing_name}/shadow/update",
                "topic_payload": cmd_payload,
            }
            try:
                logger.info(
                    "[Device #%d | %s | %s] Sending command to purifier: %s",
                    self.device_index,
                    self.ha_device_name,
                    self.thing_name,
                    updates,
                )
                self.ws.send(json.dumps(cmd))
                time.sleep(0.5)
                # Query device shadow to confirm updated state
                sync_msg = {
                    "opt": "sync_thing",
                    "sub_type": self.sub_type,
                    "thing_name": self.thing_name,
                }
                self.ws.send(json.dumps(sync_msg))
                res = self.ws.recv()
                data = json.loads(res)
                status = data.get("thing_status")
                if status:
                    if self.cached_outdoor_pm25 is not None:
                        status["outdoor_pm25"] = self.cached_outdoor_pm25
                    self.current_state = status
                    self.on_state_update(status)
                return True
            except Exception as e:
                logger.error("[Device #%d | %s] Failed to send command: %s", self.device_index, self.thing_name, e)
                self.close()
                return False

    def close(self):
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
            self.ws = None

    def run_loop(self):
        self.running = True
        while self.running:
            if not self.ws:
                if not self.connect():
                    time.sleep(5)
                    continue
            try:
                self.sync_status()
            except Exception as e:
                logger.warning("[Device #%d | %s] Sync failed: %s, reconnecting...", self.device_index, self.thing_name, e)
                self.close()
            time.sleep(SYNC_INTERVAL)


class PurifierDeviceHandler:
    """Manages Home Assistant MQTT discovery, telemetry, and commands for a single purifier."""

    def __init__(
        self,
        mqtt_client: mqtt.Client,
        token_mgr: TokenManager,
        thing_name: str,
        sub_type: str,
        nickname: str = "Air Purifier",
        device_index: int = 1,
        lat: float = None,
        lon: float = None,
    ):
        self.mqtt_client = mqtt_client
        self.thing_name = thing_name
        self.sub_type = sub_type
        self.nickname = nickname
        self.device_index = device_index
        self.lat = lat
        self.lon = lon

        # Device #1 keeps exact original naming & topics so existing HA objects never change.
        # Device #2+ gets suffixed with its number (e.g. " 2") as its own HA device & entities.
        self.suffix = "" if self.device_index == 1 else f" {self.device_index}"
        self.topic_prefix = MQTT_TOPIC_PREFIX if self.device_index == 1 else f"{MQTT_TOPIC_PREFIX}_{self.device_index}"
        self.state_topic = f"{self.topic_prefix}/state"
        self.availability_topic = f"{self.topic_prefix}/availability"

        clean_name = self.thing_name.replace("-", "_")
        self.ha_device_name = f"Zigma {self.nickname}{self.suffix}"
        self.device_info = {
            "identifiers": [f"zigma_{clean_name}"],
            "name": self.ha_device_name,
            "model": "Aerio 300",
            "manufacturer": "Zigma",
            "sw_version": "2.2.0.0108",
        }

        self.zigma = ZigmaClient(
            thing_name=self.thing_name,
            sub_type=self.sub_type,
            on_state_update=self.publish_state,
            token_manager=token_mgr,
            device_index=self.device_index,
            ha_device_name=self.ha_device_name,
            lat=self.lat,
            lon=self.lon,
        )

    def publish_discovery(self):
        logger.info(
            "[Device #%d | %s] Publishing Home Assistant MQTT Auto-Discovery entities for %s (suffix='%s')...",
            self.device_index,
            self.ha_device_name,
            self.thing_name,
            self.suffix,
        )

        fan_config = {
            "name": f"{self.nickname}{self.suffix}",
            "unique_id": f"zigma_{self.thing_name}_fan",
            "state_topic": self.state_topic,
            "command_topic": f"{self.topic_prefix}/power/set",
            "state_value_template": "{{ value_json.working_status | upper }}",
            "payload_on": "ON",
            "payload_off": "OFF",
            "percentage_state_topic": self.state_topic,
            "percentage_command_topic": f"{self.topic_prefix}/speed/set",
            "percentage_value_template": "{{ value_json.fan_level }}",
            "speed_range_min": 1,
            "speed_range_max": 3,
            "preset_mode_state_topic": self.state_topic,
            "preset_mode_command_topic": f"{self.topic_prefix}/preset_mode/set",
            "preset_mode_value_template": "{% if value_json.sleep_mode == 'on' %}sleep{% elif value_json.fan_mode == 'auto' %}auto{% else %}manual{% endif %}",
            "preset_modes": ["manual", "auto", "sleep"],
            "availability_topic": self.availability_topic,
            "device": self.device_info,
        }
        self.mqtt_client.publish(
            f"{HA_DISCOVERY_PREFIX}/fan/{self.thing_name}/fan/config",
            json.dumps(fan_config),
            retain=True,
        )

        pm25_config = {
            "name": f"PM2.5{self.suffix}",
            "unique_id": f"zigma_{self.thing_name}_pm25",
            "device_class": "pm25",
            "unit_of_measurement": "µg/m³",
            "state_class": "measurement",
            "state_topic": self.state_topic,
            "value_template": "{{ value_json.pm25 }}",
            "availability_topic": self.availability_topic,
            "device": self.device_info,
        }
        self.mqtt_client.publish(
            f"{HA_DISCOVERY_PREFIX}/sensor/{self.thing_name}/pm25/config",
            json.dumps(pm25_config),
            retain=True,
        )

        outdoor_pm25_config = {
            "name": f"Outdoor PM2.5{self.suffix}",
            "unique_id": f"zigma_{self.thing_name}_outdoor_pm25",
            "device_class": "pm25",
            "unit_of_measurement": "µg/m³",
            "state_class": "measurement",
            "state_topic": self.state_topic,
            "value_template": "{{ value_json.outdoor_pm25 }}",
            "availability_topic": self.availability_topic,
            "device": self.device_info,
        }
        self.mqtt_client.publish(
            f"{HA_DISCOVERY_PREFIX}/sensor/{self.thing_name}/outdoor_pm25/config",
            json.dumps(outdoor_pm25_config),
            retain=True,
        )

        filter_config = {
            "name": f"Filter Element Life{self.suffix}",
            "unique_id": f"zigma_{self.thing_name}_filter",
            "unit_of_measurement": "%",
            "icon": "mdi:air-filter",
            "state_topic": self.state_topic,
            "value_template": "{{ value_json.filter_left_percent }}",
            "availability_topic": self.availability_topic,
            "device": self.device_info,
        }
        self.mqtt_client.publish(
            f"{HA_DISCOVERY_PREFIX}/sensor/{self.thing_name}/filter/config",
            json.dumps(filter_config),
            retain=True,
        )

        rpm_config = {
            "name": f"Fan Speed RPM{self.suffix}",
            "unique_id": f"zigma_{self.thing_name}_fan_speed",
            "unit_of_measurement": "RPM",
            "icon": "mdi:fan",
            "state_topic": self.state_topic,
            "value_template": "{{ value_json.fan_speed }}",
            "availability_topic": self.availability_topic,
            "device": self.device_info,
        }
        self.mqtt_client.publish(
            f"{HA_DISCOVERY_PREFIX}/sensor/{self.thing_name}/fan_speed/config",
            json.dumps(rpm_config),
            retain=True,
        )

        lamp_config = {
            "name": f"Air Quality Indicator Lamp{self.suffix}",
            "unique_id": f"zigma_{self.thing_name}_round_lamp",
            "icon": "mdi:circle-color",
            "state_topic": self.state_topic,
            "value_template": "{{ value_json.round_lamp }}",
            "availability_topic": self.availability_topic,
            "device": self.device_info,
        }
        self.mqtt_client.publish(
            f"{HA_DISCOVERY_PREFIX}/sensor/{self.thing_name}/round_lamp/config",
            json.dumps(lamp_config),
            retain=True,
        )

        uv_config = {
            "name": f"UV Lamp{self.suffix}",
            "unique_id": f"zigma_{self.thing_name}_uv_lamp",
            "icon": "mdi:weather-sunny-alert",
            "state_topic": self.state_topic,
            "command_topic": f"{self.topic_prefix}/uv/set",
            "value_template": "{{ value_json.uv_lamp | upper }}",
            "payload_on": "ON",
            "payload_off": "OFF",
            "availability_topic": self.availability_topic,
            "device": self.device_info,
        }
        self.mqtt_client.publish(
            f"{HA_DISCOVERY_PREFIX}/switch/{self.thing_name}/uv_lamp/config",
            json.dumps(uv_config),
            retain=True,
        )

        lock_config = {
            "name": f"Child Lock{self.suffix}",
            "unique_id": f"zigma_{self.thing_name}_child_lock",
            "icon": "mdi:lock",
            "state_topic": self.state_topic,
            "command_topic": f"{self.topic_prefix}/lock/set",
            "value_template": "{{ value_json.safe_lock | upper }}",
            "payload_on": "ON",
            "payload_off": "OFF",
            "availability_topic": self.availability_topic,
            "device": self.device_info,
        }
        self.mqtt_client.publish(
            f"{HA_DISCOVERY_PREFIX}/switch/{self.thing_name}/child_lock/config",
            json.dumps(lock_config),
            retain=True,
        )

        logger.info(
            "[Device #%d | %s] Auto-Discovery entities published successfully.",
            self.device_index,
            self.ha_device_name,
        )

    def publish_state(self, state: dict):
        # Normalize PM2.5 fields so both pm25 and air_pm25 are present
        pm_val = state.get("pm25") if state.get("pm25") is not None else state.get("air_pm25")
        if pm_val is not None:
            state["pm25"] = pm_val
            state["air_pm25"] = pm_val

        payload = json.dumps(state)
        self.mqtt_client.publish(self.state_topic, payload, retain=True)
        logger.info(
            "Telemetry [Device #%d | %s | %s]: Status=%s, Speed=%s RPM, Level=%s, PM2.5=%s, Outdoor PM2.5=%s, Filter=%s%%, UV=%s, Lock=%s",
            self.device_index,
            self.ha_device_name,
            self.thing_name,
            state.get("working_status"),
            state.get("fan_speed"),
            state.get("fan_level"),
            pm_val,
            state.get("outdoor_pm25"),
            state.get("filter_left_percent"),
            state.get("uv_lamp"),
            state.get("safe_lock"),
        )

    def handle_command(self, topic: str, payload: str) -> bool:
        if topic == f"{self.topic_prefix}/power/set":
            cmd_val = "on" if payload.upper() == "ON" else "off"
            self.zigma.send_command({"working_status": cmd_val})
            return True

        if topic == f"{self.topic_prefix}/speed/set":
            try:
                val = int(float(payload))
                if val <= 0:
                    self.zigma.send_command({"working_status": "off"})
                else:
                    if val <= 3:
                        lvl = val
                    elif val <= 33:
                        lvl = 1
                    elif val <= 66:
                        lvl = 2
                    else:
                        lvl = 3
                    self.zigma.send_command({
                        "working_status": "on",
                        "fan_level": lvl,
                        "fan_mode": "hand",
                        "sleep_mode": "off",
                    })
            except ValueError:
                logger.error("[Device #%d | %s] Invalid speed value: %s", self.device_index, self.thing_name, payload)
            return True

        if topic == f"{self.topic_prefix}/preset_mode/set":
            mode = payload.lower()
            if mode == "sleep":
                self.zigma.send_command({"working_status": "on", "sleep_mode": "on"})
            elif mode == "auto":
                self.zigma.send_command({"working_status": "on", "sleep_mode": "off", "fan_mode": "auto"})
            elif mode == "manual":
                self.zigma.send_command({"working_status": "on", "sleep_mode": "off", "fan_mode": "hand"})
            return True

        if topic == f"{self.topic_prefix}/uv/set":
            cmd_val = "on" if payload.upper() == "ON" else "off"
            self.zigma.send_command({"uv_lamp": cmd_val})
            return True

        if topic == f"{self.topic_prefix}/lock/set":
            cmd_val = "on" if payload.upper() == "ON" else "off"
            self.zigma.send_command({"safe_lock": cmd_val})
            return True

        return False


class HomeAssistantBridge:
    def __init__(self, devices: list, token_mgr: TokenManager):
        self.token_mgr = token_mgr
        self.mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        if MQTT_USER:
            self.mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)

        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_message = self.on_mqtt_message

        self.handlers = [
            PurifierDeviceHandler(
                mqtt_client=self.mqtt_client,
                token_mgr=self.token_mgr,
                thing_name=d["thing_name"],
                sub_type=d["sub_type"],
                nickname=d["nickname"],
                device_index=d["index"],
                lat=d.get("lat"),
                lon=d.get("lon"),
            )
            for d in devices
        ]

    def on_mqtt_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            logger.info("Connected to MQTT broker successfully. Registering %d device(s)...", len(self.handlers))
            for handler in self.handlers:
                handler.publish_discovery()
                self.mqtt_client.publish(handler.availability_topic, "online", retain=True)
                client.subscribe(f"{handler.topic_prefix}/+/set")
                logger.info(
                    "[Device #%d | %s | %s] Subscribed to command topics: %s/+/set",
                    handler.device_index,
                    handler.ha_device_name,
                    handler.thing_name,
                    handler.topic_prefix,
                )
        else:
            logger.error("Failed to connect to MQTT broker, rc=%s", rc)

    def on_mqtt_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode("utf-8").strip()
        logger.info("Received MQTT message on %s: %s", topic, payload)

        for handler in self.handlers:
            if handler.handle_command(topic, payload):
                return

    def run(self):
        logger.info("Starting Zigma <-> Home Assistant Bridge for %d device(s)...", len(self.handlers))
        try:
            self.mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        except Exception as e:
            logger.error("Failed to connect to MQTT broker %s:%s: %s", MQTT_HOST, MQTT_PORT, e)

        self.mqtt_client.loop_start()

        for handler in self.handlers:
            t = threading.Thread(target=handler.zigma.run_loop, daemon=True)
            t.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping bridge...")
            for handler in self.handlers:
                self.mqtt_client.publish(handler.availability_topic, "offline", retain=True)
                handler.zigma.close()
            self.mqtt_client.loop_stop()


def log_device_assignments(devices: list):
    logger.info("=" * 72)
    logger.info("ZIGMA -> HOME ASSISTANT DEVICE ASSIGNMENTS (Persisted: %s)", DEVICE_MAP_PATH)
    logger.info("-" * 72)
    for d in devices:
        idx = d["index"]
        suffix = "" if idx == 1 else f" {idx}"
        topic_prefix = MQTT_TOPIC_PREFIX if idx == 1 else f"{MQTT_TOPIC_PREFIX}_{idx}"
        ha_dev_name = f"Zigma {d['nickname']}{suffix}"
        role_label = "PRIMARY (existing HA names preserved)" if idx == 1 else f"SECONDARY #{idx} (suffixed with '{suffix}')"
        logger.info(
            "  Device #%d [%s]:",
            idx,
            role_label,
        )
        logger.info("    - Hardware Thing Name : %s", d["thing_name"])
        logger.info("    - Zigma App Nickname  : %s", d["nickname"])
        logger.info("    - HA Device Name      : %s (identifier: zigma_%s)", ha_dev_name, d["thing_name"].replace("-", "_"))
        logger.info("    - HA Entity Names     : '%s%s', 'PM2.5%s', 'Outdoor PM2.5%s', 'UV Lamp%s', 'Child Lock%s', ...", d["nickname"], suffix, suffix, suffix, suffix, suffix)
        logger.info("    - MQTT Topic Prefix   : %s", topic_prefix)
        if d.get("lat") is not None and d.get("lon") is not None:
            logger.info("    - Weather Coordinates : lat=%s, lon=%s", d["lat"], d["lon"])
    logger.info("=" * 72)


def main():
    configured_names = [n.strip() for n in THING_NAME.split(",") if n.strip()]

    token_mgr = TokenManager(
        static_token=JWT_TOKEN,
        account=ZIGMA_ACCOUNT,
        password=ZIGMA_PASSWORD,
        calling_code=ZIGMA_CALLING_CODE,
    )
    token = token_mgr.get_token()

    discovered = []
    if token:
        discovered = auto_discover_things(token, REGION)
        if discovered:
            logger.info("Discovered %d device(s) on Zigma account.", len(discovered))

    devices = resolve_devices(discovered, configured_names)
    if not devices:
        logger.error("No devices found or configured on this Zigma account!")
        sys.exit(1)

    log_device_assignments(devices)

    bridge = HomeAssistantBridge(devices=devices, token_mgr=token_mgr)
    bridge.run()


if __name__ == "__main__":
    main()
