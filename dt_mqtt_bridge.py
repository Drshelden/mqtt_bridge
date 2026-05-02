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
    event = request.get_json(silent=True)
    if event is None:
        logger.warning("Webhook received non-JSON payload")
        return jsonify({"status": "error", "message": "Expected JSON body"}), 400

    if isinstance(event, dict):
        event_type = event.get("eventType", "unknown")
        target = event.get("targetName", "unknown")
    else:
        event_type = "unknown"
        target = "unknown"

    # targetName usually contains project/device path info
    safe_target = target.replace("/", "_")

    topic = MQTT_TOPIC

    publish_info = client.publish(
        topic,
        json.dumps(event),
        qos=1,
        retain=False
    )

    logger.info(
        "Webhook received event_type=%s target=%s topic=%s payload_bytes=%s mqtt_connected=%s publish_rc=%s",
        event_type,
        safe_target,
        topic,
        len(json.dumps(event)),
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
            "mqtt_connected": mqtt_connected,
            "publish_rc": publish_info.rc,
        }
    ), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)