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
    def __init__(self, handler: MQTTHandler, config, defaults=None):
        self.__dict__["handler"] = handler
        self.__dict__["_config"] = config
        self.handler.add_config(config)
        self.__initialize_defaults(defaults or {})

    def __initialize_defaults(self, defaults):
        """Initialize default values for MQTT attributes and synchronize to handler data."""
        for attr, value in defaults.items():
            self.handler.data[attr] = value  # Set the handler data directly
            topic = self._config.get(attr)
            if topic:
                self.handler.client.publish(topic, json.dumps(value))  # Publish immediately
            self.__dict__[attr] = value  # Set the local attribute without triggering setattr

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
        defaults = {
            "control_mode": "zero production",
            "control_mode_sp": "zero production",
            "irradiance": 100,
            "irradiance_sp": 100,
            "load_kW": 0,
            "load_kW_sp": 0
        }
        super().__init__(handler, config, defaults)

class Inverter(MQTTEngineEntity):
    def __init__(self, handler):
        config = {
            "production_kW": "inverter/production_kW",
            "p_limit": "inverter/p_limit",
            "p_limit_kW": "inverter/p_limit_kW",
            "rating_kW_sp": "inverter/rating_kW_sp",
            "rating_kW": "inverter/rating_kW",
            "status": "inverter/status"
        }
        defaults = {
            "production_kW": 0,
            "p_limit": 100,
            "p_limit_kW": 0,
            "rating_kW_sp": 0,
            "rating_kW": 0,
            "status": "ON"
        }
        super().__init__(handler, config, defaults)

class Meter(MQTTEngineEntity):
    def __init__(self, handler):
        config = {
            "power_kW": "meter/power_kW"
        }
        defaults = {
            "power_kW": 0
        }
        super().__init__(handler, config, defaults)

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
            try:
                if site.control_mode_sp in ["zero export", "zero production", "full production"]:
                    site.control_mode = site.control_mode_sp

                if isinstance(inverter.rating_kW_sp, (int, float)) and int(inverter.rating_kW_sp) >= 0:
                    inverter.rating_kW = inverter.rating_kW_sp

                if isinstance(site.load_kW_sp, (int, float)) and site.load_kW_sp >= 0:
                    site.load_kW = site.load_kW_sp

                inverter.production_kW = inverter.rating_kW * min(inverter.p_limit, site.irradiance) / 100

                if site.control_mode == "zero production":
                    inverter.p_limit = 0
                elif site.control_mode == "full production":
                    inverter.p_limit = 100
                elif site.control_mode == "zero export":
                    meter.power_kW = site.load_kW - inverter.production_kW
                    if meter.power_kW < 0:
                        export = abs(meter.power_kW)
                        p_limit_kW = inverter.production_kW - export
                        p_limit_kW = p_limit_kW if p_limit_kW >= 0 else 0
                        inverter.p_limit = p_limit_kW / inverter.rating_kW

                print('\n')
                for param in handler.data:
                    print(f'{param} {handler.data[param]}')

            except Exception as ex:
                print(f"main loop exception: {ex}")
            time.sleep(1)
    except KeyboardInterrupt:
        handler.stop()
