import threading
import time
import json
import paho.mqtt.client as mqtt


class MqttCommunicationManager:
    def __init__(self, broker_host, broker_port, username=None, password=None, update_interval=2.0):
        """
        Initialize the MQTT communication manager without specifying objects.
        Instead, you can register objects later using `add_object`.

        :param broker_host: MQTT broker hostname or IP
        :param broker_port: MQTT broker port (often 1883)
        :param username: Username for MQTT broker authentication (optional)
        :param password: Password for MQTT broker authentication (optional)
        :param update_interval: Interval in seconds to periodically publish attribute states
        """
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.username = username
        self.password = password
        self.update_interval = update_interval

        # Internal structure to hold objects and their attributes
        # Format:
        # {
        #     "obj_name1": {"obj": obj_instance, "attributes": ["attr1", "attr2"]},
        #     "obj_name2": {"obj": another_obj_instance, "attributes": ["attrA", "attrB"]}
        # }
        self.objects = {}

        # MQTT client setup
        self.client = mqtt.Client()
        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)

        # Bind event callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

        self.running = False
        self.thread = None

    def add_object(self, obj_name, obj, attributes):
        """
        Add a new object with a given name and attributes to be managed by MQTT.

        :param obj_name: A unique name for the object (used as a topic prefix)
        :param obj: The Python object instance that holds the attributes
        :param attributes: A list of attribute names (strings) for this object
        """
        self.objects[obj_name] = {
            "obj": obj,
            "attributes": attributes
        }

        # If we are already connected, subscribe to new topics immediately
        if self.client.is_connected():
            for attr in attributes:
                topic = f"{obj_name}/{attr}"
                self.client.subscribe(topic)
                print(f"Subscribed to: {topic}")

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("MQTT connected successfully.")
            # Subscribe to obj_name/attr for all registered objects
            for obj_name, data in self.objects.items():
                for attr in data["attributes"]:
                    topic = f"{obj_name}/{attr}"
                    client.subscribe(topic)
                    print(f"Subscribed to: {topic}")
        else:
            print("Failed to connect, return code:", rc)

    def on_message(self, client, userdata, msg):
        # Determine which object and attribute this message refers to
        topic_parts = msg.topic.split('/')
        if len(topic_parts) == 2:
            obj_name, attr_name = topic_parts
        else:
            # Invalid topic structure; ignoring
            return

        # Payload parsing
        payload = msg.payload.decode('utf-8')
        try:
            value = json.loads(payload)
        except json.JSONDecodeError:
            # If not JSON, treat it as raw string
            value = payload

        # Update the corresponding object's attribute if it exists
        if obj_name in self.objects:
            obj_info = self.objects[obj_name]
            if attr_name in obj_info["attributes"]:
                obj = obj_info["obj"]
                if hasattr(obj, attr_name):
                    old_value = getattr(obj, attr_name)
                    setattr(obj, attr_name, value)
                    print(f"Updated {obj_name}.{attr_name} from {old_value} to {value}")

    def on_disconnect(self, client, userdata, rc):
        print("MQTT client disconnected with return code:", rc)

    def start(self):
        """Start the MQTT client loop in a background thread."""
        self.running = True
        self.client.connect(self.broker_host, self.broker_port, 60)
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

    def stop(self):
        """Stop the MQTT client loop."""
        self.running = False
        self.client.disconnect()
        if self.thread is not None:
            self.thread.join()

    def _run_loop(self):
        # Start MQTT network loop in separate thread
        self.client.loop_start()

        # Periodically publish object states
        while self.running:
            self.publish_attributes()
            time.sleep(self.update_interval)

        self.client.loop_stop()

    def publish_attributes(self):
        # Publish each object's attributes
        for obj_name, data in self.objects.items():
            obj = data["obj"]
            for attr in data["attributes"]:
                if hasattr(obj, attr):
                    value = getattr(obj, attr)
                    payload = json.dumps(value)
                    topic = f"{obj_name}/{attr}"
                    self.client.publish(topic, payload)


# Example usage
if __name__ == "__main__":
    class MyController:
        def __init__(self):
            self.temperature = 25.5
            self.humidity = 40
            self.mode = "auto"

    class AnotherController:
        def __init__(self):
            self.speed = 100
            self.direction = "forward"

    # Create MQTT manager without specifying objects initially
    mqtt_manager = MqttCommunicationManager(
        broker_host="localhost",
        broker_port=1883,
        update_interval=1.0
    )

    # Register objects dynamically
    my_controller = MyController()
    another_controller = AnotherController()

    mqtt_manager.add_object("my_controller", my_controller, ["temperature", "humidity", "mode"])
    mqtt_manager.add_object("another_controller", another_controller, ["speed", "direction"])

    mqtt_manager.start()

    try:
        while True:
            # Simulate changing attributes over time
            my_controller.temperature += 0.1
            another_controller.speed += 1
            time.sleep(10)
    except KeyboardInterrupt:
        pass
    finally:
        mqtt_manager.stop()
