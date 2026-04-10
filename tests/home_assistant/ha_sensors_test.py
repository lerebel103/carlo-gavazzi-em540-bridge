import json

from app.home_assistant.ha_sensors import HA_AVAILABILITY_TOPIC, EnergyMeterSensor


def test_all_sensor_discovery_payloads_advertise_availability():
    sensors = EnergyMeterSensor()

    for topic, raw_payload in sensors.advertise_data():
        payload = json.loads(raw_payload)
        assert payload["availability_topic"] == HA_AVAILABILITY_TOPIC
        assert payload["payload_available"] == "online"
        assert payload["payload_not_available"] == "offline"
