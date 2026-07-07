#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "freertos/event_groups.h"
#include "esp_system.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_netif.h"
#include "nvs_flash.h"
#include "mqtt_client.h"
#include "driver/i2c_master.h"
#include "esp_log.h"
#include "esp_sntp.h"
#include "cJSON.h"

// ==========================================
// 1. CREDENTIALS & TOPICS
// ==========================================
#define WIFI_SSID                   "Hello"
#define WIFI_PASS                   "oof000fo"

#define MQTT_BROKER_URI             "mqtts://10.221.183.162:8883"
#define MQTT_USERNAME               "esp32_client"
#define MQTT_PASSWORD               "pass1234"
#define MQTT_DISCO_TOPIC            "usc/thesis/tenant-123/N001/disco" 

// Set to 5 minutes (5 * 60 * 1000) for production, or lower (e.g., 30000) for testing
#define DISCOVERY_INTERVAL_MS       (5 * 60 * 1000) 

#define MQTT_TOPIC                  "usc/thesis/tenant-123/N001/tlm"
#define MQTT_CMD_TOPIC              "usc/thesis/tenant-123/N001/cmd"

static const char *TAG = "THESIS_NODE_N001";

// ==========================================
// 2. MOSQUITTO ROOT CA 
// ==========================================
static const char *mosqmq_root_ca =
"-----BEGIN CERTIFICATE-----\n"
"MIIDETCCAfmgAwIBAgIUJ3/2Kkv3DBx1B3fmVYoubmqup0IwDQYJKoZIhvcNAQEL\n"
"BQAwGDEWMBQGA1UEAwwNTXlMb2NhbFJvb3RDQTAeFw0yNjA3MDMwNTUwNDVaFw0z\n"
"NjA2MzAwNTUwNDVaMBgxFjAUBgNVBAMMDU15TG9jYWxSb290Q0EwggEiMA0GCSqG\n"
"SIb3DQEBAQUAA4IBDwAwggEKAoIBAQCmjljvodzXNgcVhaTYlPe7qtTTrr0FlpKB\n"
"yC/zFWfYjdGkhOSinPZyoW6zOkB/rWZVNX3uexQ/Ok4VZ/xbxnaTX3Jzcf5vmSFs\n"
"BsU3XHW7iyLTkyo0XuwIGTsyLuU1lWq+tU4rRDFiK5WChea0pZk1AHvJuUAgaGvP\n"
"QJrp6VPX4HVXFYA5WNjfNlN11n0z/d/1CiHoHqQT4g7CAFXKGrpT3q+j41LnH3T4\n"
"GKd2BtVAf0KfzFmKfz8ZI9hJ/edrxqQwg0HNImNGZGjDd5ZuPREOK0uNeAEhOQ/G\n"
"RARuiOH8bCxYHeAV4MzyhcYaYzhV5LvFSJA4YxZGW2TKToIrvjRFAgMBAAGjUzBR\n"
"MB0GA1UdDgQWBBSRGyEjp0NKUACwYjjwGVNTbYS+WzAfBgNVHSMEGDAWgBSRGyEj\n"
"p0NKUACwYjjwGVNTbYS+WzAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUA\n"
"A4IBAQCWVZVa2INfpQxP/aZKh3JY036gLslcz2zthI1qODxt6s6kJjWNj05//Jip\n"
"aFW5yI/TwywoJFo35s+thCsPciXT7vxjKPk/dzqAT4ChbQ7N5jx9IIzGruAKzSZY\n"
"iMF55vbm/IzvLZC/JuEUdSXc/FHzesevFenywf5Yw3b4z36pYzZ9xTb1H74A4Ob5\n"
"7xcRhk3pPWJAe2+aNwyLf/XvbxgCNFh9xIICJjiZnijskfxM2IUTkKjbu05bKnjs\n"
"WYXVNPaE7n4+jH81eeF891k9cHQSQFIyiw90/tRnbe0ji2vth//JSsIemDdQjsOh\n"
"UVrQTwBqG65RjKR3DAvmMkP1trgg\n"
"-----END CERTIFICATE-----\n";

// ==========================================
// 3. HARDWARE CONSTANTS & STATE
// ==========================================
#define I2C_MASTER_SDA_IO           11
#define I2C_MASTER_SCL_IO           12
#define I2C_MASTER_FREQ_HZ          100000
#define REG_POINTER_CONVERT         0x00
#define REG_POINTER_CONFIG          0x01

i2c_master_bus_handle_t bus_handle;
i2c_master_dev_handle_t ads_handles[4];
uint8_t num_ads_found = 0;
esp_mqtt_client_handle_t mqtt_client = NULL;

SemaphoreHandle_t i2c_mutex;
SemaphoreHandle_t data_mutex;
bool port_active[4] = {true, false, true, false};

volatile bool is_mqtt_connected = false;

static EventGroupHandle_t s_wifi_event_group;
#define WIFI_CONNECTED_BIT          BIT0

typedef struct {
    uint8_t address;
    int16_t port_values[4];
} NodeData;

NodeData global_node_data[4];

// ==========================================
// 4. NETWORK, TIME & MQTT EVENT HANDLERS
// ==========================================
static const char *wifi_reason_str(uint8_t reason) {
    switch (reason) {
        case 2:   return "AUTH_EXPIRE";
        case 15:  return "4WAY_HANDSHAKE_TIMEOUT";
        case 201: return "NO_AP_FOUND";
        case 202: return "AUTH_FAIL";
        case 203: return "ASSOC_FAIL";
        case 204: return "HANDSHAKE_TIMEOUT";
        case 205: return "CONNECTION_FAIL";
        default:  return "UNKNOWN_REASON";
    }
}

static void wifi_event_handler(void* arg, esp_event_base_t event_base, int32_t event_id, void* event_data) {
    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
        wifi_event_sta_disconnected_t *e = (wifi_event_sta_disconnected_t *) event_data;
        ESP_LOGW(TAG, "Wi-Fi disconnected. reason=%d (%s). Reconnecting...", e->reason, wifi_reason_str(e->reason));
        xEventGroupClearBits(s_wifi_event_group, WIFI_CONNECTED_BIT);
        esp_wifi_connect();
    } else if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *ip = (ip_event_got_ip_t *) event_data;
        ESP_LOGI(TAG, "Wi-Fi connected. Got IP: " IPSTR, IP2STR(&ip->ip_info.ip));
        xEventGroupSetBits(s_wifi_event_group, WIFI_CONNECTED_BIT);
    }
}

static void mqtt_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data) {
    esp_mqtt_event_handle_t event = event_data;
    esp_mqtt_client_handle_t client = event->client;

    if (event_id == MQTT_EVENT_CONNECTED) {
        ESP_LOGI(TAG, "SUCCESS! Secure TLS Connection to Mosquitto Established!");
        is_mqtt_connected = true;
        
        // Subscribe to the command topic upon successful connection
        esp_mqtt_client_subscribe(client, MQTT_CMD_TOPIC, 1);
        ESP_LOGI(TAG, "Listening for commands on: %s", MQTT_CMD_TOPIC);

    } else if (event_id == MQTT_EVENT_DISCONNECTED) {
        ESP_LOGW(TAG, "MQTT Broker Disconnected.");
        is_mqtt_connected = false;

    } else if (event_id == MQTT_EVENT_DATA) {
        ESP_LOGI(TAG, "Command Received from Broker!");
        
        char *json_string = malloc(event->data_len + 1);
        if (json_string) {
            memcpy(json_string, event->data, event->data_len);
            json_string[event->data_len] = '\0';
            
            cJSON *root = cJSON_Parse(json_string);
            if (root) {
                cJSON *port_item = cJSON_GetObjectItem(root, "port");
                cJSON *state_item = cJSON_GetObjectItem(root, "state");

                if (port_item && state_item && cJSON_IsNumber(port_item) && cJSON_IsBool(state_item)) {
                    int port_index = port_item->valueint;
                    bool new_state = cJSON_IsTrue(state_item);
                    
                    if (port_index >= 0 && port_index < 4) {
                        port_active[port_index] = new_state;
                        ESP_LOGW(TAG, ">>> HARDWARE UPDATE: Port %d state changed to %s <<<", 
                                 port_index, new_state ? "ON" : "OFF");
                    }
                } else {
                    ESP_LOGE(TAG, "Invalid command payload format.");
                }
                cJSON_Delete(root);
            }
            free(json_string);
        }
        
    } else if (event_id == MQTT_EVENT_ERROR) {
        if (event->error_handle->error_type == MQTT_ERROR_TYPE_TCP_TRANSPORT) {
            ESP_LOGE(TAG, "MQTT TLS/transport error. esp-tls err: 0x%x, tls stack: 0x%x",
                     event->error_handle->esp_tls_last_esp_err,
                     event->error_handle->esp_tls_stack_err);
        }
    }
}

static void obtain_time(void) {
    ESP_LOGI(TAG, "Initializing SNTP to get network time for TLS...");
    esp_sntp_setoperatingmode(SNTP_OPMODE_POLL);
    esp_sntp_setservername(0, "pool.ntp.org");
    esp_sntp_init();

    int retry = 0;
    const int retry_count = 15;
    while (sntp_get_sync_status() == SNTP_SYNC_STATUS_RESET && ++retry < retry_count) {
        ESP_LOGI(TAG, "Waiting for system time to sync... (%d/%d)", retry, retry_count);
        vTaskDelay(pdMS_TO_TICKS(2000));
    }

    time_t now;
    struct tm timeinfo;
    time(&now);
    localtime_r(&now, &timeinfo);
    ESP_LOGI(TAG, "Time synced! Current Year: %d", (1900 + timeinfo.tm_year));

    if ((1900 + timeinfo.tm_year) < 2024) {
        ESP_LOGW(TAG, "Clock not synced — TLS cert validation will likely FAIL.");
    }
}

static void network_init(void) {
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_LOGW(TAG, "NVS needs erase, reinitializing...");
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    esp_netif_create_default_wifi_sta();

    s_wifi_event_group = xEventGroupCreate();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    ESP_ERROR_CHECK(esp_event_handler_instance_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL, NULL));
    ESP_ERROR_CHECK(esp_event_handler_instance_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL, NULL));

    wifi_config_t wifi_config = {
        .sta = {
            .ssid = WIFI_SSID,
            .password = WIFI_PASS,
            .pmf_cfg = {
                .capable = true,
                .required = false,
            },
        },
    };
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());

    ESP_LOGI(TAG, "Waiting for Wi-Fi / IP...");
    EventBits_t bits = xEventGroupWaitBits(s_wifi_event_group, WIFI_CONNECTED_BIT, pdFALSE, pdTRUE, pdMS_TO_TICKS(20000));

    if (bits & WIFI_CONNECTED_BIT) {
        obtain_time();
    } else {
        ESP_LOGE(TAG, "No IP after 20s. Check the disconnect reason= logged above.");
    }

    esp_mqtt_client_config_t mqtt_cfg = {
        .broker.address.uri = MQTT_BROKER_URI,
        .credentials.username = MQTT_USERNAME,
        .credentials.authentication.password = MQTT_PASSWORD,
        .broker.verification.certificate = mosqmq_root_ca,
        .broker.verification.skip_cert_common_name_check = true
    };
    mqtt_client = esp_mqtt_client_init(&mqtt_cfg);
    esp_mqtt_client_register_event(mqtt_client, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
    esp_mqtt_client_start(mqtt_client);
}

// ==========================================
// 5. HARDWARE I2C INITIALIZATION & SCANNING
// ==========================================
static void scan_i2c_bus(void) {
    const uint8_t possible_addresses[] = {0x48, 0x49, 0x4A, 0x4B};
    uint8_t found_this_run = 0;

    if (xSemaphoreTake(i2c_mutex, pdMS_TO_TICKS(200)) == pdTRUE) {
        
        for (int i = 0; i < num_ads_found; i++) {
            if (ads_handles[i] != NULL) {
                i2c_master_bus_rm_device(ads_handles[i]);
                ads_handles[i] = NULL;
            }
        }

        for (int i = 0; i < 4; i++) {
            if (i2c_master_probe(bus_handle, possible_addresses[i], 100) == ESP_OK) {
                i2c_device_config_t dev_cfg = {
                    .dev_addr_length = I2C_ADDR_BIT_LEN_7,
                    .device_address = possible_addresses[i],
                    .scl_speed_hz = I2C_MASTER_FREQ_HZ,
                };
                
                if (i2c_master_bus_add_device(bus_handle, &dev_cfg, &ads_handles[found_this_run]) == ESP_OK) {
                    if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
                        global_node_data[found_this_run].address = possible_addresses[i];
                        xSemaphoreGive(data_mutex);
                    }
                    found_this_run++;
                }
            }
        }

        if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
            num_ads_found = found_this_run;
            xSemaphoreGive(data_mutex);
        }

        xSemaphoreGive(i2c_mutex);
    }
    
    ESP_LOGI(TAG, "Active I2C Scan Complete. Online ADC Chips: %d", num_ads_found);
}

static esp_err_t i2c_master_init(void) {
    i2c_master_bus_config_t i2c_mst_config = {
        .clk_source = I2C_CLK_SRC_DEFAULT,
        .i2c_port = -1,
        .scl_io_num = I2C_MASTER_SCL_IO,
        .sda_io_num = I2C_MASTER_SDA_IO,
        .glitch_ignore_cnt = 7,
        .flags.enable_internal_pullup = true,
    };
    return i2c_new_master_bus(&i2c_mst_config, &bus_handle);
}

// ==========================================
// 6. FREERTOS TASKS & EDGE EVALUATION
// ==========================================
static uint8_t evaluate_port_status(int16_t raw_value) {
    const int16_t ADS_OVERFLOW_MAX = 32760; 
    const int16_t ADS_OVERFLOW_MIN = -32760;
    const int16_t ADS_DISCONNECTED_THRESHOLD = 5;

    // Check if communication actually failed (-9999 is set by the reader task)
    if (raw_value == -9999) {
        return 3; // Code 3 = Physical chip connection failure / I2C Timeout
    }
    if (raw_value >= ADS_OVERFLOW_MAX || raw_value <= ADS_OVERFLOW_MIN) {
        return 2; // Code 2 = Overflow / Saturation
    }
    if (raw_value >= -ADS_DISCONNECTED_THRESHOLD && raw_value <= ADS_DISCONNECTED_THRESHOLD) {
        return 1; // Code 1 = Open Circuit / Floating
    }
    return 0; 
}

void ads_reader_task(void *pvParameter) {
    while (1) {
        uint8_t local_chips_count = num_ads_found;

        for (int chip_index = 0; chip_index < local_chips_count; chip_index++) {
            if (ads_handles[chip_index] == NULL) continue;

            for (int channel = 0; channel < 4; channel++) {
                if (!port_active[channel]) continue;

                uint8_t config_msb;
                switch (channel) {
                    case 0: config_msb = 0xC3; break;
                    case 1: config_msb = 0xD3; break;
                    case 2: config_msb = 0xE3; break;
                    case 3: config_msb = 0xF3; break;
                }
                uint8_t config_data[3] = {REG_POINTER_CONFIG, config_msb, 0x83};
                uint8_t reg_pointer = REG_POINTER_CONVERT;
                uint8_t read_buf[2];

                if (xSemaphoreTake(i2c_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
                    // Send config with strict 100ms timeout
                    esp_err_t tx_err = i2c_master_transmit(ads_handles[chip_index], config_data, sizeof(config_data), pdMS_TO_TICKS(100));
                    xSemaphoreGive(i2c_mutex);

                    if (tx_err != ESP_OK) {
                        if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
                            global_node_data[chip_index].port_values[channel] = -9999;
                            xSemaphoreGive(data_mutex);
                        }
                        continue; 
                    }

                    vTaskDelay(pdMS_TO_TICKS(10));

                    if (xSemaphoreTake(i2c_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
                        // Receive data with strict 100ms timeout
                        esp_err_t rx_err = i2c_master_transmit_receive(ads_handles[chip_index], &reg_pointer, 1, read_buf, sizeof(read_buf), pdMS_TO_TICKS(100));
                        xSemaphoreGive(i2c_mutex);

                        int16_t final_val = -9999;
                        if (rx_err == ESP_OK) {
                            final_val = (read_buf[0] << 8) | read_buf[1];
                        }

                        if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
                            global_node_data[chip_index].port_values[channel] = final_val;
                            xSemaphoreGive(data_mutex);
                        }
                    }
                }
            }
        }
        vTaskDelay(pdMS_TO_TICKS(50));
    }
}

void telemetry_builder_task(void *pvParameter) {
    while (1) {
        if (!is_mqtt_connected) {
            ESP_LOGW(TAG, "Network down. Rechecking connection in 5s...");
            vTaskDelay(pdMS_TO_TICKS(5000)); 
            continue;
        }

        cJSON *root = cJSON_CreateObject();
        cJSON_AddStringToObject(root, "t", "tlm");
        cJSON_AddNumberToObject(root, "v", 1);
        cJSON_AddStringToObject(root, "tid", "tenant-123");
        cJSON_AddStringToObject(root, "nid", "N001");
        cJSON_AddNumberToObject(root, "ts", (double)time(NULL));

        cJSON *adc_array = cJSON_AddArrayToObject(root, "adc");

        if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
            for (int chip = 0; chip < num_ads_found; chip++) {
                cJSON *chip_obj = cJSON_CreateObject();

                char hex_addr[5];
                sprintf(hex_addr, "0x%02X", global_node_data[chip].address);
                cJSON_AddStringToObject(chip_obj, "a", hex_addr);

                cJSON *ports_array = cJSON_AddArrayToObject(chip_obj, "p");
                for (int channel = 0; channel < 4; channel++) {
                    if (port_active[channel]) {
                        int16_t raw_reading = global_node_data[chip].port_values[channel];
                        uint8_t current_status = evaluate_port_status(raw_reading);

                        int port_data[3] = {channel, raw_reading, current_status};
                        cJSON *single_port_arr = cJSON_CreateIntArray(port_data, 3);
                        cJSON_AddItemToArray(ports_array, single_port_arr);
                    }
                }
                cJSON_AddItemToArray(adc_array, chip_obj);
            }
            xSemaphoreGive(data_mutex);
        }

        char *payload_string = cJSON_PrintUnformatted(root);

        if (mqtt_client != NULL && payload_string != NULL) {
            int msg_id = esp_mqtt_client_publish(mqtt_client, MQTT_TOPIC, payload_string, 0, 1, 0);
            if (msg_id != -1) {
                ESP_LOGI(TAG, "Telemetry Sent to Mosquitto: %s", payload_string);
            }
        }

        free(payload_string);
        cJSON_Delete(root);
        
        vTaskDelay(pdMS_TO_TICKS(10000));
    }
}

void discovery_builder_task(void *pvParameter) {
    while (1) {
        if (!is_mqtt_connected) {
            vTaskDelay(pdMS_TO_TICKS(5000));
            continue;
        }

        scan_i2c_bus();

        cJSON *root = cJSON_CreateObject();
        cJSON_AddStringToObject(root, "t", "disco");
        cJSON_AddNumberToObject(root, "v", 1);
        cJSON_AddStringToObject(root, "tid", "tenant-123");
        cJSON_AddStringToObject(root, "nid", "N001");
        cJSON_AddNumberToObject(root, "ts", (double)time(NULL));

        cJSON *bus_array = cJSON_AddArrayToObject(root, "buses");
        
        if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
            cJSON_AddNumberToObject(root, "detected_chips", num_ads_found);
            for (int chip = 0; chip < num_ads_found; chip++) {
                char hex_addr[5];
                sprintf(hex_addr, "0x%02X", global_node_data[chip].address);
                cJSON_AddItemToArray(bus_array, cJSON_CreateString(hex_addr));
            }
            xSemaphoreGive(data_mutex);
        }

        char *payload_string = cJSON_PrintUnformatted(root);

        if (mqtt_client != NULL && payload_string != NULL) {
            int msg_id = esp_mqtt_client_publish(mqtt_client, "usc/thesis/tenant-123/N001/disco", payload_string, 0, 1, 1);
            if (msg_id != -1) {
                ESP_LOGW(TAG, ">>> Discovery Audit Dispatched: %s <<<", payload_string);
            }
        }

        free(payload_string);
        cJSON_Delete(root);

        // Fixed: The loop now correctly uses the macro defined at the top of the file
        vTaskDelay(pdMS_TO_TICKS(DISCOVERY_INTERVAL_MS));
    }
}

// ==========================================
// 7. APP MAIN ENTRY
// ==========================================
void app_main(void) {
    i2c_mutex = xSemaphoreCreateMutex();
    data_mutex = xSemaphoreCreateMutex();

    network_init();

    if (i2c_master_init() != ESP_OK) {
        ESP_LOGE(TAG, "Failed to initialize I2C master peripheral engine. Node halting.");
        vTaskSuspend(NULL);
    }

    scan_i2c_bus();

    xTaskCreate(ads_reader_task, "unified_adc_worker", 3072, NULL, 5, NULL);
    xTaskCreate(telemetry_builder_task, "tlm_json_task", 4096, NULL, 5, NULL);
    xTaskCreate(discovery_builder_task, "disco_json_task", 4096, NULL, 5, NULL);
}