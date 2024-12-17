import paho.mqtt.client as mqtt
import json
import time

class MQTTHandler:
    def __init__(self, broker, port, listen_publish_config=None, publish_interval=5):
        self.broker = broker
        self.port = port
        self._config = listen_publish_config or {}  # Internal config dict for topics
        self.publish_interval = publish_interval  # Time interval for publishing (in seconds)
        self.data = {attr: None for attr in self._config.keys()}  # Store incoming data by attribute
        self.last_update_time = {attr: None for attr in self._config.keys()}  # Track last update time by attribute
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, rc):
        print("Connected with result code " + str(rc))
        for attr, topic in self._config.items():
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
            attr = next((a for a, t in self._config.items() if t == msg.topic), None)
            if attr:
                self.data[attr] = payload
                self.last_update_time[attr] = time.time()  # Update the last received time
        except Exception as e:
            print(f"Error processing message from {msg.topic}: {e}")

    def connect(self):
        self.client.connect(self.broker, self.port, 60)

    def add_config(self, new_config):
        """Add new configuration entries to the MQTTHandler."""
        for attr, topic in new_config.items():
            self._config[attr] = topic
            self.data[attr] = None
            self.last_update_time[attr] = None
            self.client.subscribe(topic)
            print(f"Added and subscribed to {topic} for {attr}")

    def start(self):
        self.connect()
        self.client.loop_start()

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()

class MQTTEngineEntity:
    def __init__(self, handler: MQTTHandler, config):
        self.handler = handler
        self._config = config
        self.handler.add_config(config)

    def __setattr__(self, name, value):
        """Intercept attribute setting to send updates to MQTT."""
        if "_config" in self.__dict__ and name in self._config:
            self.handler.data[name] = value
            topic = self.handler._config.get(name)
            if topic:
                self.handler.client.publish(topic, json.dumps(value))
        super().__setattr__(name, value)

    def __getattr__(self, name):
        """Intercept attribute getting to retrieve the latest data from MQTT."""
        if name in self.handler.data:
            return self.handler.data[name]  # Always fetch from handler.data
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

class Site(MQTTEngineEntity):
    def __init__(self, handler):
        config = {
            "control_mode": "site/control_mode",
            "irradiance": "site/irradiance",
            "load_kW": "site/load_kW",
            "control_mode_sp": "site/control_mode_sp",
            "irradiance_sp": "site/irradiance_sp",
            "load_kW_sp": "site/load_kW_sp"
        }
        super().__init__(handler, config)

class Inverter(MQTTEngineEntity):
    def __init__(self, handler):
        config = {
            "production_kW": "inverter/production_kW",
            "p_limit": "inverter/p_limit",
            "status": "inverter/status"
        }
        super().__init__(handler, config)

class Meter(MQTTEngineEntity):
    def __init__(self, handler):
        config = {
            "power": "meter/power"
        }
        super().__init__(handler, config)

# Example usage
if __name__ == "__main__":
    handler = MQTTHandler(
        broker="127.0.0.1",
        port=1883,
        publish_interval=5
    )

    site = Site(handler)
    inverter = Inverter(handler)
    meter = Meter(handler)

    handler.start()

    try:
        while True:
            # Example: Access data for each entity
            print(f"Site Load: {site.load_kW}")
            print(f"Inverter Production: {inverter.production_kW}")
            print(f"Meter Power: {meter.power}")
            time.sleep(1)
    except KeyboardInterrupt:
        handler.stop()
