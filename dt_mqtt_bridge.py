from flask import Flask, request, jsonify
import paho.mqtt.client as mqtt
import json
import os

app = Flask(__name__)

MQTT_HOST = os.getenv("MQTT_HOST", "io.adafruit.com")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASS = os.getenv("MQTT_PASS")

client = mqtt.Client()

if MQTT_USER:
    client.username_pw_set(MQTT_USER, MQTT_PASS)

client.connect(MQTT_HOST, MQTT_PORT, 60)
client.loop_start()


@app.get("/health")
def health():
    return jsonify({"ok": True}), 200

@app.route("/dt-webhook", methods=["POST"])
def dt_webhook():
    event = request.json

    event_type = event.get("eventType", "unknown")
    target = event.get("targetName", "unknown")

    # targetName usually contains project/device path info
    safe_target = target.replace("/", "_")

    #topic = f"dt/events/{safe_target}/{event_type}"
    topic = "dennisshelden/feeds/rfidscans"

    client.publish(
        topic,
        json.dumps(event),
        qos=1,
        retain=False
    )

    return jsonify({"status": "ok", "published_topic": topic}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)