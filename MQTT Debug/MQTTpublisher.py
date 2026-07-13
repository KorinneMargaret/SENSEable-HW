import json
import ssl
import sys
import time
import uuid
import paho.mqtt.client as mqtt

MQTT_HOST = "10.66.206.162"
MQTT_PORT = 8883
MQTT_USER = "admin_user"
MQTT_PASS = "adminpass1234"
CA_CERT_PATH = "C:/MQTT_Certs/ca.crt"
MQTT_CMD_TOPIC = "usc/thesis/tenant-123/N001/cmd"

def build_base_payload(action: str) -> dict:
    return {
        "t": "cmd", "v": 1, "tid": "tenant-123", "nid": "N001",
        "cid": str(uuid.uuid4()), "ts": int(time.time()), "action": action
    }

def interactive_menu() -> dict:
    print("\n" + "="*40 + "\n--- SELECT TARGET ACTION ---\n" + "="*40)
    print("[1] Bit-Bang Bus Recovery (Clear Frozen I2C Lines)")
    print("[2] Actuator Driving (Binary or PWM Regulation)")
    print("[3] Manual Sensor Port Configuration Override (Up / Down)")
    print("[Q] Quit Publisher Script")
    choice = input("Enter option (1, 2, 3, or Q): ").strip().lower()

    if choice == 'q': return "QUIT"

    if choice == "1":
        payload = build_base_payload("bus_recovery")
        try:
            bus_id = int(input("Target I2C Bus ID Index (0 or 1): ").strip())
            if bus_id not in [0, 1]: raise ValueError("Bus ID must be 0 or 1.")
            payload["bus_id"] = bus_id
            return payload
        except ValueError as e:
            print(f"[INPUT ERROR] {e}"); return None

    elif choice == "2":
        payload = build_base_payload("actuate")
        try:
            # UPDATED: Allows mapping to all 6 physical ESP32 actuators
            p_val = int(input("Target Physical Output Channel (1-6): OUT").strip())
            if p_val < 1 or p_val > 6: 
                raise ValueError("Firmware currently supports actuators OUT1 through OUT6.")
            payload["port"] = p_val 
        except ValueError as e:
            print(f"[INPUT ERROR] {e}")
            return None

        mode = input("Select Driver Signal Mode ('bin' for Relay/Switch, 'pwm' for Motor/LED): ").strip().lower()
        if mode not in ["bin", "pwm"]: 
            print("[INPUT ERROR] Mode must be exactly 'bin' or 'pwm'.")
            return None
        payload["mode"] = mode

        if mode == "bin":
            state = input("Set Logic Condition (0 for OFF, 1 for ON): ").strip()
            if state not in ["0", "1"]: 
                return None
            payload["state"] = int(state)
        else:
            try:
                duty = int(input("Set 8-bit PWM Duty Cycle (0 = Off, 128 = 50%, 255 = 100%): ").strip())
                if duty < 0 or duty > 255: 
                    raise ValueError("PWM Duty must be an integer between 0 and 255.")
                payload["duty"] = duty
            except ValueError as e:
                print(f"[INPUT ERROR] {e}")
                return None

        try:
            dur = int(input("Execution Window Timeout in Milliseconds (0 for Indefinite hold): ").strip())
            if dur < 0: raise ValueError()
            payload["dur"] = dur
        except ValueError: 
            return None
            
        return payload

    elif choice == "3":
        print("\n--- MANUAL SENSOR PORT OVERRIDE ---")
        action_choice = input("Select operation direction ('up' or 'down'): ").strip().lower()
        if action_choice not in ["up", "down"]: return None
        
        payload = build_base_payload(f"sensor_port_{action_choice}")
        try:
            chip_idx = int(input("Target Chip Address Index (0=0x48, 1=0x49, 2=0x4A, 3=0x4B): ").strip())
            ch_idx = int(input("Target ADS1115 Multiplexer Channel Index (0-3): ").strip())
            if chip_idx not in range(4) or ch_idx not in range(4):
                raise ValueError("Indices must be between 0 and 3.")
            payload["chip"] = chip_idx
            payload["ch"] = ch_idx
            return payload
        except ValueError as e:
            print(f"[INPUT ERROR] {e}"); return None
            
    return None

def main():
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(username=MQTT_USER, password=MQTT_PASS)
    try:
        client.tls_set(ca_certs=CA_CERT_PATH, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
        client.tls_insecure_set(True)
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        client.loop_start()
        time.sleep(0.5)
    except Exception as e:
        print(f"[CRITICAL] Connection broke down: {e}"); sys.exit(1)

    try:
        while True:
            payload_dict = interactive_menu()
            if payload_dict == "QUIT": break
            if not payload_dict: continue

            payload_str = json.dumps(payload_dict)
            msg_info = client.publish(MQTT_CMD_TOPIC, payload_str, qos=1)
            msg_info.wait_for_publish()

            if msg_info.rc == mqtt.MQTT_ERR_SUCCESS:
                print("\n[SUCCESS] Packet dispatched successfully!")
            else:
                print(f"\n[FAILURE] Error code: {msg_info.rc}")
            
            if input("\nDo you want to send another command? (y/n): ").strip().lower() != 'y': break
    finally:
        client.loop_stop(); client.disconnect()

if __name__ == "__main__": main()


