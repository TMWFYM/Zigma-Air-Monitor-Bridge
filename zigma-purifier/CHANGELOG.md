# Changelog

## 1.0.4
- Added multi-device support: automatically discovers and bridges all purifiers registered to the account.
- Preserved exact Home Assistant device and entity names/topics for Device #1 so existing setups and automations remain unchanged.
- Suffixed Device #2+ with its device number (e.g. `2`) as a separate Home Assistant device with its own controls and sensors.
- Added persistent device index mapping (`/data/device_map.json`) and detailed startup assignment logging so device order changes from the cloud API across restarts never swap Home Assistant objects.

## 1.0.3
- Fixed fan speed control: mapped discrete speed levels (1, 2, 3) and percentage inputs to purifier fan levels.
- Updated Home Assistant MQTT fan percentage template to match configured `speed_range_min` and `speed_range_max`.
- Fixed preset mode template string comparisons for `sleep` and `auto` modes.
- Turning speed to 0% now turns purifier off, and setting a speed takes device out of sleep mode.

## 1.0.2
- Integrated Grit Cloud Weather API to dynamically query live outdoor PM2.5 based on registered device coordinates.
- Replaces static/cached shadow outdoor PM2.5 with live meteorology station reports matching the official Zigma app.
- Fallback to device shadow outdoor PM2.5 if coordinates or weather service are unavailable.

## 1.0.1
- Added Home Assistant UI translation schema in `translations/en.yaml`.
- Fixed indoor PM2.5 sensor key mapping (`pm25` vs `air_pm25`).
- Added Outdoor PM2.5 sensor entity to Home Assistant discovery.

## 1.0.0
- Initial release of the Zigma Aerio 300 Home Assistant Add-on and MQTT bridge.
