# Zigma Aerio 300 Smart Air Purifier - Home Assistant Add-on & MQTT Bridge

[![Home Assistant Add-on](https://img.shields.io/badge/Home%20Assistant-Add--on-blue.svg)](https://www.home-assistant.io/)
[![Docker Image Size](https://img.shields.io/badge/Docker%20Size-~28%20MB-brightgreen.svg)]()
[![License](https://img.shields.io/badge/License-MIT-green.svg)]()

A lightweight, high-performance bridge that connects the **Zigma Aerio 300** smart air purifier to **Home Assistant** via MQTT with native Auto-Discovery.

Supports both **Home Assistant OS / Supervised Add-on** and **Standalone Docker / Docker Compose**.

---

## Highlights

- **Native Home Assistant MQTT Auto-Discovery:** No manual YAML configuration required; entities automatically appear under MQTT integration.
- **Automatic Device Discovery:** Leave device ID blank and the bridge automatically queries your Zigma account and discovers your purifier.
- **Dual Authentication:**
  - Automated Cloud Login via your Zigma email/phone and password (hashes passwords with MD5 as required by Grit Cloud).
  - Optional static JWT token support with automatic fallback to credentials on expiration.
- **Full Bi-directional Control:**
  - Power (ON / OFF)
  - Speed (Levels 1, 2, 3 mapped to percentage 33%, 66%, 100%)
  - Modes: Manual, Auto, Sleep
  - UV-C sterilization lamp toggle
  - Child lock toggle
- **Real-Time Telemetry Streaming:**
  - Indoor PM2.5 particulate levels (µg/m³)
  - Filter Lifespan percentage (%)
  - Fan Speed RPM
  - Air Quality Indicator Lamp ring status
- **Ultra-lightweight:** Built on Python 3.11 Alpine (~28MB container footprint).

---

## Discovered Entities in Home Assistant

| Entity | Domain | Description |
|---|---|---|
| `Air Purifier` | `fan` | Main fan entity: Power, Speed %, Preset modes (manual, auto, sleep) |
| `PM2.5` | `sensor` | Indoor air quality particulate sensor (µg/m³) |
| `Outdoor PM2.5` | `sensor` | Real-time outdoor air quality from local weather stations (µg/m³) |
| `Filter Element Life` | `sensor` | Remaining filter element life percentage (%) |
| `Fan Speed RPM` | `sensor` | Real-time motor rotation speed |
| `Air Quality Indicator Lamp` | `sensor` | Ring lamp color status |
| `UV Lamp` | `switch` | UV-C sterilization toggle |
| `Child Lock` | `switch` | Physical button child lock toggle |

---

## Installation Option 1: Home Assistant Add-on

### Method A: Custom Add-on Repository (Recommended)
1. In Home Assistant, go to **Settings** -> **Add-ons** -> **Add-on Store**.
2. Click the three dots in the top right corner and select **Repositories**.
3. Add the repository URL: `https://github.com/TMWFYM/Zigma-Air-Monitor-Bridge`
4. Refresh the Add-on Store, find **Zigma Aerio 300 Purifier**, and click **Install**.
5. Go to the **Configuration** tab, enter your Zigma `account` and `password`, and click **Save**.
6. Start the add-on and enable **Start on boot** and **Watchdog**.

### Method B: Local Add-on
1. Copy the `zigma-purifier/` folder into your Home Assistant `/addons/` directory (e.g. `/addons/zigma-purifier`).
2. Go to **Settings** -> **Add-ons** -> **Add-on Store**, click the three dots, and select **Check for updates**.
3. Install **Zigma Aerio 300 Purifier** under the Local Add-ons section.
4. Fill in your Zigma credentials under the **Configuration** tab and start.

---

## Installation Option 2: Standalone Docker Compose

If you run Home Assistant Core or prefer deploying via Docker / Portainer / unRAID / TrueNAS:

1. Clone or copy this repository to your server.
2. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```
3. Edit `.env` with your details:
   ```bash
   # MQTT Configuration
   MQTT_HOST=192.168.1.100
   MQTT_PORT=1883
   MQTT_USER=your_mqtt_user
   MQTT_PASSWORD=your_mqtt_password

   # Zigma Cloud Credentials
   ZIGMA_ACCOUNT=your_email_or_phone
   ZIGMA_PASSWORD=your_password
   ZIGMA_CALLING_CODE=001

   # Device Name (leave blank to auto-discover)
   THING_NAME=
   ```
4. Build and start the container:
   ```bash
   docker compose up -d
   ```
5. Check live logs:
   ```bash
   docker compose logs -f
   ```

---

