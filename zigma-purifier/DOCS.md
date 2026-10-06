# Home Assistant Add-on: Zigma Aerio 300 Purifier

Bi-directional MQTT bridge and real-time sensor telemetry for **Zigma Aerio 300** smart air purifiers.

---

## Features

- **Home Assistant MQTT Auto-Discovery:** Instantly creates fan, sensors, and switches in Home Assistant.
- **Real-Time Telemetry:**
  - PM2.5 particulate count (µg/m³)
  - Filter Lifespan (%)
  - Fan Speed RPM
  - Air Quality Ring Lamp status
- **Bi-Directional Fan Control:**
  - Turn On / Off
  - Speed levels (Level 1: 33%, Level 2: 66%, Level 3: 100%)
  - Modes: Manual, Auto, Sleep
  - UV-C sterilization lamp toggle
  - Child lock toggle
- **Auto-Discovery:** Automatically finds and binds to the Aerio 300 purifier registered on your Zigma account.
- **Dual Authentication:** Supports automated account login (email/phone & password) or static JWT token with automatic token refresh.

---

## Configuration

Navigate to the **Configuration** tab in the add-on:

### 1. Zigma Cloud Credentials

- **`account`**: Your Zigma login phone number or email.
- **`password`**: Your Zigma account password.
- **`calling_code`**: Country calling code (e.g. `001` for US/Canada, `044` for UK). Default: `001`.
- **`thing_name`**: *(Optional)* Your purifier device name (e.g. `aerio-300-xx-xx-xx-xx-xx-xx`). **Leave blank to automatically discover the device registered to your account.**
- **`jwt_token`**: *(Optional)* Static JWT token if you prefer not to store credentials.

### 2. MQTT Broker

- **`mqtt_host`**: Hostname of your MQTT broker. If using the official Home Assistant Mosquitto broker add-on, keep the default `core-mosquitto`.
- **`mqtt_port`**: MQTT broker port (default: `1883`).
- **`mqtt_user`**: MQTT username (leave empty if anonymous).
- **`mqtt_password`**: MQTT password.

---

## Discovered Entities

Once started, the add-on automatically registers the following entities in Home Assistant:

| Entity | Domain | Description |
|---|---|---|
| `Air Purifier` | `fan` | Main purifier entity (Power, Speed %, Modes: manual, auto, sleep) |
| `PM2.5` | `sensor` | Indoor air quality particulate count (µg/m³) |
| `Filter Element Life` | `sensor` | Remaining filter lifespan percentage (%) |
| `Fan Speed RPM` | `sensor` | Real-time motor rotation speed |
| `Air Quality Indicator Lamp` | `sensor` | Ring lamp color status |
| `UV Lamp` | `switch` | UV-C sterilization lamp |
| `Child Lock` | `switch` | Physical button lock on device |
