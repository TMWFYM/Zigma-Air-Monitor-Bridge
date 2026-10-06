# Changelog

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
