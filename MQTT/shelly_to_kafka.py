import json
import logging
from kafka import KafkaProducer
import paho.mqtt.client as mqtt

# --- KONFIGURATION ---
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
# Shelly Gen 1 verwendet oft 'shellies/shellyplug-s-<ID>/relay/0'
# Shelly Gen 2/3 (Plus/Pro) verwendet 'shellyplusplug-<ID>/status/switch:0'
MQTT_TOPIC = "#" #shellies/+/relay/0"

KAFKA_BROKER = "kfk1.mipemnet.com:9092"
KAFKA_TOPIC = "shelly"

# Logging einrichten

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ShellyKafkaBridge")

# --- KAFKA INITIALISIERUNG ---
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    logger.info("Erfolgreich mit Kafka verbunden.")
except Exception as e:
    logger.error(f"Kafka-Verbindung fehlgeschlagen: {e}")
    exit(1)


# --- MQTT CALLBACKS ---
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info("Erfolgreich mit MQTT Broker verbunden.")
        client.subscribe(MQTT_TOPIC)
        logger.info(f"Topic abonniert: {MQTT_TOPIC}")
    else:
        logger.error(f"MQTT Verbindung fehlgeschlagen mit Code {rc}")


def on_message(client, userdata, msg):
    try:
        # Payload dekodieren
        payload_str = msg.payload.decode("utf-8")
        print(payload_str)
        # Versuche JSON zu parsen (Gen 2/3), andernfalls als String (Gen 1)
        try:
            payload_data = json.loads(payload_str)
        except json.JSONDecodeError:
            payload_data = payload_str

        # Nachricht für Kafka vorbereiten
        kafka_message = {"mqtt_topic": msg.topic, "data": payload_data}

        # An Kafka senden
        producer.send(KAFKA_TOPIC, value=kafka_message)
        producer.flush()

        logger.info(f"Nachricht erfolgreich an Kafka weitergeleitet [{KAFKA_TOPIC}]")

    except Exception as e:
        logger.error(f"Fehler beim Verarbeiten der Nachricht: {e}")


# --- MAIN LOOP ---
def main():
    # paho-mqtt v2.x erfordert die Angabe der API-Version
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        logger.info(f"Verbinde mit MQTT Broker ({MQTT_BROKER}:{MQTT_PORT})...")
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_forever()
    except KeyboardInterrupt:
        logger.info("Skript wird beendet...")
    finally:
        client.disconnect()
        producer.close()


if __name__ == "__main__":
    main()
