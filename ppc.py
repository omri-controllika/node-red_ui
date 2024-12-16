import paho.mqtt.client as mqtt
import json
import threading
import time

class MQTTHandler:
    def __init__(self, broker, port, listen_publish_config, publish_interval):
        self.broker = broker
        self.port = port
        self.config = listen_publish_config  # Dict mapping attribute names to MQTT topics
        self.publish_interval = publish_interval  # Time interval for publishing (in seconds)
        self.data = {attr: None for attr in self.config.keys()}  # Store incoming data by attribute
        self.last_update_time = {attr: None for attr in self.config.keys()}  # Track last update time by attribute
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self._stop_event = threading.Event()

    def on_connect(self, client, userdata, flags, rc):
        print("Connected with result code " + str(rc))
        for attr, topic in self.config.items():
            client.subscribe(topic)
            print(f"Subscribed to {topic} for {attr}")

    def on_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode("utf-8")
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                pass  # Keep as string if not JSON

            # Find the attribute corresponding to this topic
            attr = next((a for a, t in self.config.items() if t == msg.topic), None)
            if attr:
                self.data[attr] = payload
                self.last_update_time[attr] = time.time()  # Update the last received time
        except Exception as e:
            print(f"Error processing message from {msg.topic}: {e}")

    def connect(self):
        self.client.connect(self.broker, self.port, 60)

    def publish_background(self):
        while not self._stop_event.is_set():
            for attr, topic in self.config.items():
                try:
                    value = self.data.get(attr)  # Fetch value directly from the data dictionary
                    if value is not None:
                        self.client.publish(topic, json.dumps(value))
                except Exception as e:
                    print(f"Error publishing message to {topic}: {e}")
            time.sleep(self.publish_interval)

    def start(self):
        self.connect()
        self.client.loop_start()
        self.publish_thread = threading.Thread(target=self.publish_background)
        self.publish_thread.start()

    def stop(self):
        self._stop_event.set()
        self.publish_thread.join()
        self.client.loop_stop()
        self.client.disconnect()

# Example usage
if __name__ == "__main__":
    # Configuration for attributes and their corresponding MQTT topics
    config = {
        "control_mode_sp": "site/control_mode_sp",
        "irradiance_sp": "site/irradiance_sp",
        "load_sp": "site/load_sp",
        "power": "meter/power",
        "load_power": "site/load_power",
        "irradiance": "site/irradiance",
        "control_mode": "site/control_mode",
        "production_kW": "pv/production_kW",
        "p_limit": "pv/p_limit",
        "p_limit_kW": "pv/p_limit_kW"
    }

    handler = MQTTHandler(
        broker="127.0.0.1",
        port=1883,
        listen_publish_config=config,
        publish_interval=5
    )

    handler.start()

    try:
        while True:
            # Example: Access the latest data by attributes and their last update time
            for attr, value in handler.data.items():
                last_update = handler.last_update_time.get(attr, 0)
                if last_update:
                    last_update = round(last_update, 1)
                print(f"{attr}: {value} (Last updated: {last_update})")
            handler.data["control_mode"] = handler.data["control_mode_sp"]
            time.sleep(0.15)
            print('\n')
    except KeyboardInterrupt:
        handler.stop()
