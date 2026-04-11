import json

from app.home_assistant.ha_sensors import HA_AVAILABILITY_TOPIC, EnergyMeterSensor


def test_all_sensor_discovery_payloads_advertise_availability():
    sensors = EnergyMeterSensor()

    for topic, raw_payload in sensors.advertise_data():
        payload = json.loads(raw_payload)
        assert payload["availability_topic"] == HA_AVAILABILITY_TOPIC
        assert payload["payload_available"] == "online"
        assert payload["payload_not_available"] == "offline"


def test_topic_prefix_is_applied_to_sensor_topics_and_ids():
    sensors = EnergyMeterSensor(topic_prefix="qa/test-stack")

    topic, raw_payload = sensors.frequency.discovery()
    payload = json.loads(raw_payload)

    assert topic.startswith("homeassistant/")
    assert payload["state_topic"].startswith("qa/test-stack/lerebel/")
    assert payload["availability_topic"].startswith("qa/test-stack/lerebel/")
    assert payload["name"] == "Frequency"
    assert payload["unique_id"].startswith("em540_bridge_qa_test_stack_")
    assert payload["device"]["identifiers"] == ["em540_bridge_qa_test_stack"]
    assert payload["device"]["model"] == "[qa/test-stack] EM540 Bridge"
