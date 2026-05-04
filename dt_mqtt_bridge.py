from flask import Flask, request, jsonify
import paho.mqtt.client as mqtt
import json
import os
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dt-mqtt-bridge")

MQTT_HOST = os.getenv("MQTT_HOST", "io.adafruit.com")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASS = os.getenv("MQTT_PASS")
MQTT_TOPIC = os.getenv(
    "MQTT_TOPIC",
    f"{MQTT_USER}/feeds/rfidscans" if MQTT_USER else "rfidscans",
)

mqtt_connected = False


def extract_sensor_name(payload, event, metadata):
    for source in (payload, event, metadata):
        if not isinstance(source, dict):
            continue

        labels = source.get("labels")
        if isinstance(labels, dict):
            for key in ("name", "sensor", "device"):
                value = labels.get(key)
                if value:
                    return str(value)

        for key in ("targetName", "sensorName", "deviceName", "name"):
            value = source.get(key)
            if value:
                return str(value).split("/")[-1]

    device_id = metadata.get("deviceId") if isinstance(metadata, dict) else None
    if device_id:
        return str(device_id)

    return "unknown"


def extract_event_value(data, event_type):
    if not isinstance(data, dict):
        return data

    if event_type in data:
        event_payload = data[event_type]
        if isinstance(event_payload, dict) and "value" in event_payload:
            return event_payload.get("value")
        if isinstance(event_payload, dict) and "state" in event_payload:
            return event_payload.get("state")
        return event_payload

    for value in data.values():
        if isinstance(value, dict) and "value" in value:
            return value.get("value")
        if isinstance(value, (str, int, float, bool)):
            return value

    return "unknown"


def extract_event_id(payload, event, metadata):
    for source in (event, payload, metadata):
        if not isinstance(source, dict):
            continue

        for key in ("eventId", "id", "deviceId"):
            value = source.get(key)
            if value:
                return str(value)

    return "unknown"


def on_connect(client, userdata, flags, reason_code, properties=None):
    global mqtt_connected
    mqtt_connected = reason_code == 0
    logger.info(
        "MQTT connect result rc=%s host=%s port=%s user_set=%s",
        reason_code,
        MQTT_HOST,
        MQTT_PORT,
        bool(MQTT_USER),
    )


def on_disconnect(client, userdata, reason_code, properties=None):
    global mqtt_connected
    mqtt_connected = False
    logger.warning("MQTT disconnected rc=%s", reason_code)

client = mqtt.Client()
client.on_connect = on_connect
client.on_disconnect = on_disconnect

if MQTT_USER:
    client.username_pw_set(MQTT_USER, MQTT_PASS)

try:
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_start()
except Exception as exc:
    logger.exception("Initial MQTT connection failed: %s", exc)


@app.get("/health")
def health():
    return jsonify({"ok": True, "mqtt_connected": mqtt_connected}), 200


@app.get("/")
def index():
    return jsonify(
        {
            "service": "dt-mqtt-bridge",
            "status": "running",
            "endpoints": ["/health", "/dt-webhook"],
        }
    ), 200

@app.route("/dt-webhook", methods=["POST"])
def dt_webhook():
    logger.info(
        "Webhook request received content_type=%s content_length=%s",
        request.content_type,
        request.content_length,
    )

    event = request.get_json(silent=True)
    if event is None:
        # Some senders post JSON with non-application/json content types.
        raw_body = request.get_data(as_text=True)
        try:
            event = json.loads(raw_body) if raw_body else None
        except json.JSONDecodeError:
            event = None

    if event is None:
        logger.warning("Webhook received non-JSON payload")
        return jsonify({"status": "error", "message": "Expected JSON body"}), 400

    if not isinstance(event, dict):
        logger.warning("Webhook received JSON payload that was not an object")
        return jsonify({"status": "error", "message": "Expected JSON object"}), 400

    event_body = event.get("event", event)
    metadata = event.get("metadata", {})
    if not isinstance(event_body, dict):
        logger.warning("Webhook received object without event body")
        return jsonify({"status": "error", "message": "Expected event object"}), 400

    event_type = event_body.get("eventType", "unknown")
    target = event_body.get("targetName", event.get("targetName", "unknown"))

    # targetName usually contains project/device path info
    safe_target = target.replace("/", "_")
    sensor_name = extract_sensor_name(event, event_body, metadata)
    sensor_value = extract_event_value(event_body.get("data", {}), event_type)
    event_id = extract_event_id(event, event_body, metadata)
    payload = f"{sensor_name},{sensor_value},{event_id}"

    topic = MQTT_TOPIC

    publish_info = client.publish(
        topic,
        payload,
        qos=1,
        retain=False
    )

    logger.info(
        "Webhook received event_type=%s target=%s sensor_name=%s sensor_value=%s event_id=%s topic=%s payload=%s mqtt_connected=%s publish_rc=%s",
        event_type,
        safe_target,
        sensor_name,
        sensor_value,
        event_id,
        topic,
        payload,
        mqtt_connected,
        publish_info.rc,
    )

    if publish_info.rc != mqtt.MQTT_ERR_SUCCESS:
        return jsonify(
            {
                "status": "error",
                "message": "MQTT publish failed",
                "publish_rc": publish_info.rc,
                "mqtt_connected": mqtt_connected,
            }
        ), 502

    return jsonify(
        {
            "status": "ok",
            "published_topic": topic,
            "published_payload": payload,
            "mqtt_connected": mqtt_connected,
            "publish_rc": publish_info.rc,
        }
    ), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)