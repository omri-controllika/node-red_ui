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
        "load_kW_sp": "site/load_kW_sp",
        "meter_power_kW": "meter/meter_power_kW",
        "load_kW": "site/load_kW",
        "irradiance": "site/irradiance",
        "control_mode": "site/control_mode",
        "production_kW": "pv/production_kW",
        "p_limit": "pv/p_limit",
        "p_limit_kW": "pv/p_limit_kW",
        "pv_rating_kW": "pv/rating_kW"
    }

    handler = MQTTHandler(
        broker="127.0.0.1",
        port=1883,
        listen_publish_config=config,
        publish_interval=5
    )

    handler.start()

    control_modes = ['zero export', 'full production', 'zero production']

    handler.data["control_mode_sp"] = 'full production'
    handler.data["irradiance_sp"] = 100
    handler.data["load_kW_sp"] = 100
    handler.data["meter_power_kW"] = 0
    handler.data["load_kW"] = 0
    handler.data["irradiance"] = 100 
    handler.data["control_mode"] = 'full production'
    handler.data["production_kW"] = 0
    handler.data["p_limit"] = 0
    handler.data["p_limit_kW"] = 0
    handler.data["pv_rating_kW"] = 500

    try:
        while True:

            if handler.data["control_mode_sp"] in control_modes:
                handler.data["control_mode"] = handler.data["control_mode_sp"]
            else:
                # todo log
                pass

            control_mode_sp = handler.data["control_mode_sp"]
            irradiance_sp = handler.data["irradiance_sp"]
            load_kW_sp = handler.data["load_kW_sp"]
            meter_power_kW = handler.data["meter_power_kW"]
            load_kW = load_kW_sp
            irradiance = irradiance_sp
            handler.data["irradiance"] = irradiance
            control_mode = handler.data["control_mode"]
            production_kW = handler.data["production_kW"]
            p_limit = handler.data["p_limit"]
            # p_limit_kW = handler.data["p_limit_kW"]
            pv_rating_kW = handler.data["pv_rating_kW"]

            handler.data["load_kW"] = load_kW

            meter_power_kW = load_kW - production_kW
            handler.data["meter_power_kW"] = meter_power_kW

            if control_mode == 'full production':
                p_limit = 100
            elif control_mode == 'zero production':
                p_limit = 0
            elif control_mode == 'zero export':
                # todo
                pass

            p_limit_kW = p_limit * pv_rating_kW /100
            handler.data["p_limit_kW"] = p_limit_kW
            handler.data["p_limit"] = p_limit

            production = pv_rating_kW * min(p_limit, irradiance)/100
            handler.data["production_kW"] = production

            time.sleep(0.15)
            print('\n')
    except KeyboardInterrupt:
        handler.stop()
