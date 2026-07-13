import json
import ssl
import sys
import time
import paho.mqtt.client as mqtt

# ==============================================================================
# CONFIGURATION (Updated Credentials)
# ==============================================================================
MQTT_HOST = "10.150.75.37"  # Updated to recent broker address
MQTT_PORT = 8883
MQTT_USER = "admin_user"
MQTT_PASS = "adminpass1234"
CA_CERT_PATH = "C:/MQTT_Certs/ca.crt"

# Subscribing to all sub-topics under Node 001 (Telemetry, Discovery, etc.)
MQTT_SUBSCRIBE_TOPIC = "usc/thesis/tenant-123/N001/#"


# ==============================================================================
# PAHO MQTT V2 CALLBACK FUNCTIONS
# ==============================================================================
def on_connect(client, userdata, flags, rc, properties=None):
    """Callback execution upon successful broker handshake connection."""
    if rc == 0:
        print("\n[SUCCESS] Connected securely to Mosquitto Broker via TLS!")
        print(f"[NET] Registering subscription path: {MQTT_SUBSCRIBE_TOPIC}")
        client.subscribe(MQTT_SUBSCRIBE_TOPIC, qos=1)
        print("[STATUS] Awaiting incoming edge-node data frames...\n" + "-"*60)
    else:
        print(f"[ERROR] Connection handshake rejected. Status Code: {rc}")


def on_message(client, userdata, msg):
    """Callback intercepting incoming payload streams across subscribed branches."""
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    print(f"\n[{timestamp}] Incoming Data on Topic: {msg.topic}")
    print(f"QoS: {msg.qos} | Retained: {msg.retain}")
    
    try:
        # Decode binary packet layout array back to raw string sequence
        raw_payload = msg.payload.decode('utf-8')
        
        # Parse and pretty-print JSON contents matching node schemas
        json_data = json.loads(raw_payload)
        print("-" * 60)
        print(json.dumps(json_data, indent=4))
        print("-" * 60)
        
    except json.JSONDecodeError:
        print("[WARNING] Malformed non-JSON data stream string sequence caught:")
        print(msg.payload)
    except Exception as e:
        print(f"[ERROR] Failed to process incoming envelope: {e}")


def on_disconnect(client, userdata, disconnect_flags, rc, properties=None):
    """Failsafe trace catching physical connection dropped events."""
    print(f"\n[WARNING] Secure link disconnected from broker. Status Code: {rc}")


# ==============================================================================
# MAIN EXECUTION INTERFACE
# ==============================================================================
def main():
    print("=" * 60)
    print(" USC THESIS SECURE EDGE-NODE TELEMETRY DASHBOARD")
    print("=" * 60)
    print(f"[INIT] Initializing monitoring client for host: {MQTT_HOST}...")

    # Instantiate secure client tracking mapping matching current Paho API definitions
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(username=MQTT_USER, password=MQTT_PASS)

    # Attach tracking functions safely to runtime loop triggers
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    # Configure Secure TLS Layer Encapsulation Context
    try:
        client.tls_set(
            ca_certs=CA_CERT_PATH,
            certfile=None,
            keyfile=None,
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
            ciphers=None
        )
        # Bypasses local testing hostname validation mismatches seamlessly
        client.tls_insecure_set(True) 
    except FileNotFoundError:
        print(f"\n[CRITICAL] Root CA certificate missing at target path: {CA_CERT_PATH}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[CRITICAL] TLS initialization breakdown: {e}")
        sys.exit(1)

    # Begin persistent monitoring socket link path
    try:
        print(f"[NET] Establishing secure network loop socket channel...")
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        
        # Blocking loop runs indefinitely, processing network events automatically
        client.loop_forever()
        
    except KeyboardInterrupt:
        print("\n\n[INFO] Manual termination call detected. Shutting down dashboard.")
    except Exception as e:
        print(f"\n[CRITICAL] Run encountered a delivery pipeline exception: {e}")
    finally:
        client.disconnect()
        print("[STATUS] Monitoring interface closed safely. Execution complete.\n")


if __name__ == "__main__":
    main()