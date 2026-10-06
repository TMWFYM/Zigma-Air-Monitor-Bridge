#!/usr/bin/env python3
"""
Zigma Aerio 300 <-> Home Assistant MQTT Bridge
Bi-directional real-time telemetry streaming and control over MQTT.
Supports Home Assistant Add-on (/data/options.json), Docker, and standalone execution.
Supports both static JWT token and automated username/password login.
Auto-discovers purifiers on account if no device ID is specified.
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


class TokenManager:
    """Manages JWT tokens: checks validity and falls back to username/password login."""

    def __init__(self, static_token="", account="", password="", calling_code="001"):
        self.static_token = static_token.strip()
        self.account = account.strip()
        self.password = password.strip()
        self.calling_code = calling_code.strip()
        self.active_token = self.static_token
        self.token_expiry = 0

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


def get_outdoor_weather(token: str, lat: float, lon: float, region: str = "ap-southeast-1") -> dict:
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
            return data["data"]
    except Exception as e:
        logger.debug("Failed to query Grit Cloud weather API: %s", e)
    return {}


class ZigmaClient:
    def __init__(self, thing_name, sub_type, on_state_update, token_manager: TokenManager, lat=None, lon=None):
        self.thing_name = thing_name
        self.sub_type = sub_type
        self.on_state_update = on_state_update
        self.token_manager = token_manager
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
            logger.error("Cannot connect: No JWT token available.")
            return False

        headers = {"token": token, "region": REGION}
        try:
            logger.info("Connecting to Zigma Cloud WebSocket (%s)...", WSS_URL)
            self.ws = websocket.create_connection(
                WSS_URL,
                header=headers,
                sslopt={"cert_reqs": ssl.CERT_NONE},
                timeout=10,
            )
            logger.info("Connected to Zigma Cloud WebSocket.")
            self.sync_status()
            return True
        except Exception as e:
            logger.error("Failed to connect to WebSocket: %s", e)
            self.ws = None
            if self.token_manager.account and self.token_manager.password:
                self.token_manager.token_expiry = 0
                self.token_manager.active_token = ""
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
                                logger.info("Live Outdoor PM2.5 updated from weather service: %s µg/m³", self.cached_outdoor_pm25)
                            self.last_weather_check = now
                        except Exception as ex:
                            logger.debug("Weather query skipped: %s", ex)

                    if self.cached_outdoor_pm25 is not None:
                        status["outdoor_pm25"] = self.cached_outdoor_pm25

                    self.current_state = status
                    self.on_state_update(status)
            except Exception as e:
                logger.error("Error during sync_status: %s", e)
                self.close()

    def send_command(self, updates: dict):
        with self.lock:
            if not self.ws and not self.connect():
                logger.error("Cannot send command: WebSocket disconnected.")
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
                logger.info("Sending command to purifier (%s): %s", self.thing_name, updates)
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
                logger.error("Failed to send command: %s", e)
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
                logger.warning("Sync failed: %s, reconnecting...", e)
                self.close()
            time.sleep(SYNC_INTERVAL)


class HomeAssistantBridge:
    def __init__(self, thing_name, sub_type, nickname="Air Purifier", lat=None, lon=None):
        self.thing_name = thing_name
        self.sub_type = sub_type
        self.nickname = nickname
        self.lat = lat
        self.lon = lon
        self.state_topic = f"{MQTT_TOPIC_PREFIX}/state"
        self.availability_topic = f"{MQTT_TOPIC_PREFIX}/availability"

        clean_name = self.thing_name.replace("-", "_")
        self.device_info = {
            "identifiers": [f"zigma_{clean_name}"],
            "name": f"Zigma {self.nickname}",
            "model": "Aerio 300",
            "manufacturer": "Zigma",
            "sw_version": "2.2.0.0108",
        }

        self.mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        if MQTT_USER:
            self.mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)

        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_message = self.on_mqtt_message

        self.token_mgr = TokenManager(
            static_token=JWT_TOKEN,
            account=ZIGMA_ACCOUNT,
            password=ZIGMA_PASSWORD,
            calling_code=ZIGMA_CALLING_CODE,
        )
        self.zigma = ZigmaClient(
            thing_name=self.thing_name,
            sub_type=self.sub_type,
            on_state_update=self.publish_state,
            token_manager=self.token_mgr,
            lat=self.lat,
            lon=self.lon
        )

    def on_mqtt_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            logger.info("Connected to MQTT broker successfully.")
            self.publish_discovery()
            self.mqtt_client.publish(self.availability_topic, "online", retain=True)
            client.subscribe(f"{MQTT_TOPIC_PREFIX}/+/set")
            logger.info("Subscribed to command topics: %s/+/set", MQTT_TOPIC_PREFIX)
        else:
            logger.error("Failed to connect to MQTT broker, rc=%s", rc)

    def publish_discovery(self):
        logger.info("Publishing Home Assistant MQTT Auto-Discovery entities for %s...", self.thing_name)

        fan_config = {
            "name": "Air Purifier",
            "unique_id": f"zigma_{self.thing_name}_fan",
            "state_topic": self.state_topic,
            "command_topic": f"{MQTT_TOPIC_PREFIX}/power/set",
            "state_value_template": "{{ value_json.working_status | upper }}",
            "payload_on": "ON",
            "payload_off": "OFF",
            "percentage_state_topic": self.state_topic,
            "percentage_command_topic": f"{MQTT_TOPIC_PREFIX}/speed/set",
            "percentage_value_template": "{{ ((value_json.fan_level / 3) * 100) | round }}",
            "speed_range_min": 1,
            "speed_range_max": 3,
            "preset_mode_state_topic": self.state_topic,
            "preset_mode_command_topic": f"{MQTT_TOPIC_PREFIX}/preset_mode/set",
            "preset_mode_value_template": "{% if value_json.sleep_mode == on %}sleep{% elif value_json.fan_mode == auto %}auto{% else %}manual{% endif %}",
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
            "name": "PM2.5",
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
            "name": "Outdoor PM2.5",
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
            "name": "Filter Element Life",
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
            "name": "Fan Speed RPM",
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
            "name": "Air Quality Indicator Lamp",
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
            "name": "UV Lamp",
            "unique_id": f"zigma_{self.thing_name}_uv_lamp",
            "icon": "mdi:weather-sunny-alert",
            "state_topic": self.state_topic,
            "command_topic": f"{MQTT_TOPIC_PREFIX}/uv/set",
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
            "name": "Child Lock",
            "unique_id": f"zigma_{self.thing_name}_child_lock",
            "icon": "mdi:lock",
            "state_topic": self.state_topic,
            "command_topic": f"{MQTT_TOPIC_PREFIX}/lock/set",
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

        logger.info("Auto-Discovery entities published successfully.")

    def publish_state(self, state: dict):
        # Normalize PM2.5 fields so both pm25 and air_pm25 are present
        pm_val = state.get("pm25") if state.get("pm25") is not None else state.get("air_pm25")
        if pm_val is not None:
            state["pm25"] = pm_val
            state["air_pm25"] = pm_val

        payload = json.dumps(state)
        self.mqtt_client.publish(self.state_topic, payload, retain=True)
        logger.info(
            "Telemetry [%s]: Status=%s, Speed=%s RPM, Level=%s, PM2.5=%s, Outdoor PM2.5=%s, Filter=%s%%, UV=%s, Lock=%s",
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

    def on_mqtt_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode("utf-8").strip()
        logger.info("Received MQTT message on %s: %s", topic, payload)

        if topic == f"{MQTT_TOPIC_PREFIX}/power/set":
            cmd_val = "on" if payload.upper() == "ON" else "off"
            self.zigma.send_command({"working_status": cmd_val})

        elif topic == f"{MQTT_TOPIC_PREFIX}/speed/set":
            try:
                pct = int(payload)
                if pct <= 33:
                    lvl = 1
                elif pct <= 66:
                    lvl = 2
                else:
                    lvl = 3
                self.zigma.send_command({"working_status": "on", "fan_level": lvl, "fan_mode": "hand"})
            except ValueError:
                logger.error("Invalid speed percentage: %s", payload)

        elif topic == f"{MQTT_TOPIC_PREFIX}/preset_mode/set":
            mode = payload.lower()
            if mode == "sleep":
                self.zigma.send_command({"working_status": "on", "sleep_mode": "on"})
            elif mode == "auto":
                self.zigma.send_command({"working_status": "on", "sleep_mode": "off", "fan_mode": "auto"})
            elif mode == "manual":
                self.zigma.send_command({"working_status": "on", "sleep_mode": "off", "fan_mode": "hand"})

        elif topic == f"{MQTT_TOPIC_PREFIX}/uv/set":
            cmd_val = "on" if payload.upper() == "ON" else "off"
            self.zigma.send_command({"uv_lamp": cmd_val})

        elif topic == f"{MQTT_TOPIC_PREFIX}/lock/set":
            cmd_val = "on" if payload.upper() == "ON" else "off"
            self.zigma.send_command({"safe_lock": cmd_val})

    def run(self):
        logger.info("Starting Zigma <-> Home Assistant Bridge...")
        try:
            self.mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        except Exception as e:
            logger.error("Failed to connect to MQTT broker %s:%s: %s", MQTT_HOST, MQTT_PORT, e)

        self.mqtt_client.loop_start()

        zigma_thread = threading.Thread(target=self.zigma.run_loop, daemon=True)
        zigma_thread.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping bridge...")
            self.mqtt_client.publish(self.availability_topic, "offline", retain=True)
            self.mqtt_client.loop_stop()
            self.zigma.close()


def main():
    global THING_NAME, SUB_TYPE

    lat, lon = None, None
    temp_mgr = TokenManager(
        static_token=JWT_TOKEN,
        account=ZIGMA_ACCOUNT,
        password=ZIGMA_PASSWORD,
        calling_code=ZIGMA_CALLING_CODE,
    )
    token = temp_mgr.get_token()

    if token:
        devices = auto_discover_things(token, REGION)
        if devices:
            if not THING_NAME:
                matched = [
                    d for d in devices
                    if "aerio" in d.get("sub_type", "").lower() or "aerio" in d.get("thing_name", "").lower()
                ]
                chosen = matched[0] if matched else devices[0]
                THING_NAME = chosen.get("thing_name")
                SUB_TYPE = chosen.get("sub_type", "aerio-300")
                nickname = chosen.get("thing_nickname", "Air Purifier")
                logger.info(
                    "Auto-discovered purifier: '%s' (Model: %s, Sub-Type: %s)",
                    nickname,
                    chosen.get("model"),
                    SUB_TYPE,
                )
            else:
                chosen = next((d for d in devices if d.get("thing_name") == THING_NAME), devices[0])
                nickname = chosen.get("thing_nickname", "Air Purifier")

            coords = chosen.get("lng_lat", {})
            try:
                lat = float(coords.get("latitude"))
                lon = float(coords.get("longitude"))
                logger.info("Purifier coordinates for weather station: lat=%s, lon=%s", lat, lon)
            except (TypeError, ValueError):
                pass
        else:
            if not THING_NAME:
                logger.error("No devices found on this Zigma account!")
                sys.exit(1)
            nickname = "Air Purifier"
    else:
        if not THING_NAME:
            logger.error("Could not obtain JWT token or credentials to discover devices.")
            sys.exit(1)
        nickname = "Air Purifier"

    bridge = HomeAssistantBridge(thing_name=THING_NAME, sub_type=SUB_TYPE, nickname=nickname, lat=lat, lon=lon)
    bridge.run()


if __name__ == "__main__":
    main()
