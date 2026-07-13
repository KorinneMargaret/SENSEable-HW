/**
 * Project: Environment-Agnostic IoT Monitoring Framework
 * Author: Korinne Margaret V. Sasil
 * Institute: University of San Carlos, Talamban Campus
 */

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
#include "driver/gpio.h"
#include "driver/ledc.h"
#include "esp_log.h"
#include "esp_netif_sntp.h"
#include "cJSON.h"

// ==========================================
// 1. CREDENTIALS & TOPICS
// ==========================================
#define WIFI_SSID                   "Hello"
#define WIFI_PASS                   "oof000fo"

#define MQTT_BROKER_URI             "mqtts://10.66.206.162:8883"
#define MQTT_USERNAME               "esp32_client"
#define MQTT_PASSWORD               "pass1234"
#define MQTT_DISCO_TOPIC            "usc/thesis/tenant-123/N001/disco" 
#define DISCOVERY_INTERVAL_MS       (10 * 1000) 
#define MQTT_TOPIC                  "usc/thesis/tenant-123/N001/tlm"
#define MQTT_CMD_TOPIC              "usc/thesis/tenant-123/N001/cmd"

static const char *TAG = "THESIS_NODE_N001";

#define FLOATING_LEAK_MIN   4500
#define FLOATING_LEAK_MAX   5000

// ==========================================
// 2. EXPANDED ACTUATOR PIN MAPS & MUX LAYOUT
// ==========================================
#define NUM_ACTUATORS       6  

const int actuator_gpios[NUM_ACTUATORS] = {4, 5, 13, 14, 16, 17}; 

const ledc_channel_t actuator_channels[NUM_ACTUATORS] = {
    LEDC_CHANNEL_0,
    LEDC_CHANNEL_1,
    LEDC_CHANNEL_2,
    LEDC_CHANNEL_3,
    LEDC_CHANNEL_4,
    LEDC_CHANNEL_5
};

#define ACTUATOR_LEDC_MODE          LEDC_LOW_SPEED_MODE
#define ACTUATOR_LEDC_TIMER         LEDC_TIMER_0
#define ACTUATOR_LEDC_RES           LEDC_TIMER_8_BIT   
#define ACTUATOR_LEDC_FREQ          5000               

// ==========================================
// 3. MOSQUITTO ROOT CA
// ==========================================
static const char *mosqmq_root_ca =
"-----BEGIN CERTIFICATE-----\n"
"MIIDETCCAfmgAwIBAgIUCkmlV1hNXPxtmln8bwPav6nR/mYwDQYJKoZIhvcNAQEL\n"
"BQAwGDEWMBQGA1UEAwwNTXlMb2NhbFJvb3RDQTAeFw0yNjA3MTMwNzM3MzVaFw0z\n"
"NjA3MTAwNzM3MzVaMBgxFjAUBgNVBAMMDU15TG9jYWxSb290Q0EwggEiMA0GCSqG\n"
"SIb3DQEBAQUAA4IBDwAwggEKAoIBAQCc3lyIBBR6tOK8GYHySkcyBDMLY2MRW6ef\n"
"KDXMvwtepLR6g/dsCB57K9NuA1f3bRwqYy++JXWkyuB3POysEUuh3sXCR7EGYZ5j\n"
"Tsts325bFmy3pfbHDbHavCUyhGGvhRxjuGGBs/MRUwCXcoX3VW3kwU7nfDxAsFoZ\n"
"odg6JEt71SNHvbMlw4uS+5AGBQkSz5muYLF3pH4hJ4YgJSgJ4+OM/auS4iaiiseA\n"
"HQQ/DTbGvUMzAD4TKB9e90uctHgsQrLZoGDBgxnwjRElF66haH/JBYiyWlhpIXN3\n"
"XQHKa51KMYYEKQFiHKm/9vSHMRrtLn43DJTjPvTpJxeu/73LNKpHAgMBAAGjUzBR\n"
"MB0GA1UdDgQWBBS8T/yjry4S3pdyxQwGJ1fz8CoSTjAfBgNVHSMEGDAWgBS8T/yj\n"
"ry4S3pdyxQwGJ1fz8CoSTjAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUA\n"
"A4IBAQB63q/eip2FlFKe/cVNY9zYSqpON6jxjxjeCIYkNXvjQTrXlOqp/UsWRRx4\n"
"PcaGMog/U30Y8dD9CffnGJBU0EMRVc2WKWCMicwUaZVbji6KhQ/aCLakZaayAEzl\n"
"xIuVEPCnj70nFh4JrYDdct5XL6EYIE0OSzRPKUbyvq4Fx/w5bAV4yvDTBFkJPLfb\n"
"pJagz/EmFrJc1bv5oEl0/HEFJMwkjSwKvU1E29ILDTI06M5IgFULWi3XTbzJ2IbA\n"
"UwVORVzEd9TtoqVJDE9peanLCbfbjxucqbx8kJesVDgu0pMaUV9BSJ6xDy1qebdn\n"
"h/I+BAg5m86aIX7m2TvvpikIoklG\n"
"-----END CERTIFICATE-----\n";

// ==========================================
// 4. HARDWARE CONSTANTS & GLOBAL STATE
// ==========================================
#define I2C_MASTER_SDA_IO           11
#define I2C_MASTER_SCL_IO           12
#define I2C_MASTER_FREQ_HZ          100000
#define REG_POINTER_CONVERT         0x00
#define REG_POINTER_CONFIG          0x01

i2c_master_bus_handle_t bus_handle;
i2c_master_dev_handle_t ads_handles[4] = {NULL, NULL, NULL, NULL};
const uint8_t possible_addresses[4] = {0x48, 0x49, 0x4A, 0x4B};

uint8_t num_ads_found = 0;
esp_mqtt_client_handle_t mqtt_client = NULL;

SemaphoreHandle_t i2c_mutex;
SemaphoreHandle_t data_mutex;

bool port_active[4][4] = {
    {true, true, true, true},
    {true, true, true, true},
    {true, true, true, true},
    {true, true, true, true}
};

volatile bool is_mqtt_connected = false;

static EventGroupHandle_t s_wifi_event_group;
#define WIFI_CONNECTED_BIT          BIT0

static EventGroupHandle_t s_hardware_event_group;
#define I2C_RESCAN_REQUIRED_BIT     BIT0

typedef struct {
    uint8_t address;
    int16_t port_values[4];
    bool is_online;
} NodeData;

NodeData global_node_data[4];

static esp_err_t i2c_master_init(void);

// ==========================================
// BIT-BANG I2C BUS RECOVERY ROUTINE
// ==========================================
static void recover_i2c_bus(void) {
    ESP_LOGW(TAG, "Executing structured software bit-bang recovery routine...");

    if (xSemaphoreTake(i2c_mutex, portMAX_DELAY) == pdTRUE) {
        for (int i = 0; i < 4; i++) {
            if (ads_handles[i] != NULL) {
                i2c_master_bus_rm_device(ads_handles[i]);
                ads_handles[i] = NULL;
            }
        }
        if (bus_handle != NULL) {
            i2c_del_master_bus(bus_handle);
            bus_handle = NULL;
        }

        gpio_config_t bb_cfg = {
            .pin_bit_mask = (1ULL << I2C_MASTER_SDA_IO) | (1ULL << I2C_MASTER_SCL_IO),
            .mode = GPIO_MODE_INPUT_OUTPUT_OD, 
            .pull_up_en = GPIO_PULLUP_ENABLE,
            .pull_down_en = GPIO_PULLDOWN_DISABLE,
            .intr_type = GPIO_INTR_DISABLE
        };
        gpio_config(&bb_cfg);

        gpio_set_level(I2C_MASTER_SDA_IO, 1);
        for (int i = 0; i < 9; i++) {
            gpio_set_level(I2C_MASTER_SCL_IO, 0);
            vTaskDelay(pdMS_TO_TICKS(5));
            gpio_set_level(I2C_MASTER_SCL_IO, 1);
            vTaskDelay(pdMS_TO_TICKS(5));
        }

        gpio_set_level(I2C_MASTER_SDA_IO, 0);
        vTaskDelay(pdMS_TO_TICKS(5));
        gpio_set_level(I2C_MASTER_SCL_IO, 1);
        vTaskDelay(pdMS_TO_TICKS(5));
        gpio_set_level(I2C_MASTER_SDA_IO, 1);
        vTaskDelay(pdMS_TO_TICKS(5));

        if (i2c_master_init() == ESP_OK) {
            ESP_LOGI(TAG, "Hardware core I2C registers restored.");
        } else {
            ESP_LOGE(TAG, "Fatal fault re-instantiating core hardware I2C master bus.");
        }

        xSemaphoreGive(i2c_mutex);
    }
}

// ==========================================
// HARDWARE INITIALIZATION FOR ACTUATOR DRIVERS
// ==========================================
static void init_actuators(void) {
    ledc_timer_config_t ledc_timer = {
        .speed_mode       = ACTUATOR_LEDC_MODE,
        .timer_num        = ACTUATOR_LEDC_TIMER,
        .duty_resolution  = ACTUATOR_LEDC_RES,
        .freq_hz          = ACTUATOR_LEDC_FREQ,
        .clk_cfg          = LEDC_AUTO_CLK
    };
    ESP_ERROR_CHECK(ledc_timer_config(&ledc_timer));

    for (int i = 0; i < NUM_ACTUATORS; i++) {
        gpio_config_t io_conf = {
            .pin_bit_mask = (1ULL << actuator_gpios[i]),
            .mode = GPIO_MODE_INPUT_OUTPUT,
            .pull_up_en = GPIO_PULLUP_DISABLE,
            .pull_down_en = GPIO_PULLDOWN_ENABLE,
            .intr_type = GPIO_INTR_DISABLE
        };
        gpio_config(&io_conf);
        gpio_set_level(actuator_gpios[i], 0); 

        ledc_channel_config_t ledc_channel = {
            .speed_mode     = ACTUATOR_LEDC_MODE,
            .channel        = actuator_channels[i],
            .timer_sel      = ACTUATOR_LEDC_TIMER,
            .intr_type      = LEDC_INTR_DISABLE,
            .gpio_num       = actuator_gpios[i],
            .duty           = 0,
            .hpoint         = 0
        };
        ESP_ERROR_CHECK(ledc_channel_config(&ledc_channel));
    }
    ESP_LOGI(TAG, "Hardware driver arrays linked and operational (OUT1-OUT6).");
}

// ==========================================
// BACKGROUND AUTO-SHUTOFF TIMER
// ==========================================
typedef struct {
    int target_idx;
    int duration_ms;
} auto_shutoff_args_t;

void auto_shutoff_task(void *pvParameter) {
    auto_shutoff_args_t *args = (auto_shutoff_args_t *)pvParameter;
    
    // Hold execution for the specified duration window
    vTaskDelay(pdMS_TO_TICKS(args->duration_ms));
    
    int mapped_gpio = actuator_gpios[args->target_idx];
    ledc_channel_t mapped_chan = actuator_channels[args->target_idx];
    
    // Return hardware to 0V failsafe state
    ledc_stop(ACTUATOR_LEDC_MODE, mapped_chan, 0);
    gpio_set_level(mapped_gpio, 0);
    
    ESP_LOGW(TAG, ">>> Auto-shutoff triggered for OUT%d after %d ms <<<", args->target_idx + 1, args->duration_ms);
    
    free(args);
    vTaskDelete(NULL);
}

// ==========================================
// NETWORK, TIME & MQTT EVENT HANDLERS
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
        ESP_LOGW(TAG, "Wi-Fi disconnected: reason=%d (%s). Reconnecting...", e->reason, wifi_reason_str(e->reason));
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
        xEventGroupSetBits(s_hardware_event_group, I2C_RESCAN_REQUIRED_BIT);
        esp_mqtt_client_subscribe(client, MQTT_CMD_TOPIC, 1);
    } 
    else if (event_id == MQTT_EVENT_DISCONNECTED) {
        ESP_LOGW(TAG, "MQTT Broker Disconnected.");
        is_mqtt_connected = false;
    } 
    else if (event_id == MQTT_EVENT_DATA) {
        char *json_string = malloc(event->data_len + 1);
        if (json_string) {
            memcpy(json_string, event->data, event->data_len);
            json_string[event->data_len] = '\0';
            
            cJSON *root = cJSON_Parse(json_string);
            if (root) {
                cJSON *act_item = cJSON_GetObjectItem(root, "action");
                if (act_item && cJSON_IsString(act_item)) {
                    const char *action = act_item->valuestring;
                    
                    if (strcmp(action, "bus_recovery") == 0) {
                        cJSON *bus_id_item = cJSON_GetObjectItem(root, "bus_id");
                        int target_bus = (bus_id_item && cJSON_IsNumber(bus_id_item)) ? bus_id_item->valueint : 0;
                        
                        ESP_LOGI(TAG, "Schema payload validated for Bus %d. Executing recovery routine...", target_bus);
                        recover_i2c_bus();
                        xEventGroupSetBits(s_hardware_event_group, I2C_RESCAN_REQUIRED_BIT);
                    } 
                    
                    else if (strcmp(action, "actuate") == 0) {
                        cJSON *port_item = cJSON_GetObjectItem(root, "port");
                        cJSON *mode_item = cJSON_GetObjectItem(root, "mode");
                        cJSON *dur_item = cJSON_GetObjectItem(root, "dur"); 
                        
                        if (port_item && mode_item && dur_item && cJSON_IsNumber(port_item) && cJSON_IsString(mode_item)) {
                            
                            int target_idx = port_item->valueint - 1; 
                            const char *mode_str = mode_item->valuestring;
                            int duration_ms = cJSON_IsNumber(dur_item) ? dur_item->valueint : 0;

                            if (target_idx >= 0 && target_idx < NUM_ACTUATORS) { 
                                int mapped_gpio = actuator_gpios[target_idx];
                                ledc_channel_t mapped_chan = actuator_channels[target_idx];

                                if (strcmp(mode_str, "bin") == 0) {
                                    cJSON *state_item = cJSON_GetObjectItem(root, "state");
                                    if (state_item && cJSON_IsNumber(state_item)) {
                                        int state_val = state_item->valueint;
                                        
                                        ledc_stop(ACTUATOR_LEDC_MODE, mapped_chan, state_val);
                                        gpio_set_level(mapped_gpio, state_val);
                                        ESP_LOGI(TAG, "Binary: OUT%d (GPIO %d) -> %d [Duration: %d ms]", 
                                                 target_idx + 1, mapped_gpio, state_val, duration_ms);
                                    }
                                } 
                                else if (strcmp(mode_str, "pwm") == 0) {
                                    cJSON *duty_item = cJSON_GetObjectItem(root, "duty");
                                    if (duty_item && cJSON_IsNumber(duty_item)) {
                                        int duty_val = duty_item->valueint;
                                        
                                        ledc_set_duty(ACTUATOR_LEDC_MODE, mapped_chan, duty_val);
                                        ledc_update_duty(ACTUATOR_LEDC_MODE, mapped_chan);
                                        ESP_LOGI(TAG, "PWM: OUT%d (GPIO %d) -> Duty: %d/255 [Duration: %d ms]", 
                                                 target_idx + 1, mapped_gpio, duty_val, duration_ms);
                                    }
                                }
                                
                                // Spin up background shut-off timer if requested
                                if (duration_ms > 0) {
                                    auto_shutoff_args_t *args = malloc(sizeof(auto_shutoff_args_t));
                                    args->target_idx = target_idx;
                                    args->duration_ms = duration_ms;
                                    xTaskCreate(auto_shutoff_task, "auto_shutoff", 2048, (void *)args, 5, NULL);
                                }
                                
                            } else {
                                ESP_LOGW(TAG, "Actuation rejected. Port '%d' out of bounds.", target_idx + 1);
                            }
                        }
                    }

                    else if (strcmp(action, "sensor_port_up") == 0 || strcmp(action, "sensor_port_down") == 0) {
                        cJSON *chip_item = cJSON_GetObjectItem(root, "chip");
                        cJSON *ch_item = cJSON_GetObjectItem(root, "ch");
                        
                        if (chip_item && ch_item && cJSON_IsNumber(chip_item) && cJSON_IsNumber(ch_item)) {
                            int chip_idx = chip_item->valueint;
                            int ch_idx = ch_item->valueint;
                            
                            if (chip_idx >= 0 && chip_idx < 4 && ch_idx >= 0 && ch_idx < 4) {
                                bool set_active = (strcmp(action, "sensor_port_up") == 0);
                                if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
                                    port_active[chip_idx][ch_idx] = set_active;
                                    xSemaphoreGive(data_mutex);
                                }
                                ESP_LOGW(TAG, "Altered configuration: Chip Index [%d] Port [%d] -> %s", 
                                         chip_idx, ch_idx, set_active ? "ENABLED" : "DISABLED");
                                xEventGroupSetBits(s_hardware_event_group, I2C_RESCAN_REQUIRED_BIT);
                            }
                        }
                    }
                }
                cJSON_Delete(root);
            }
            free(json_string);
        }
    }
}

static void obtain_time(void) {
    ESP_LOGI(TAG, "Initializing new v6.0 SNTP API...");
    esp_sntp_config_t config = ESP_NETIF_SNTP_DEFAULT_CONFIG("pool.ntp.org");
    esp_netif_sntp_init(&config);

    ESP_LOGI(TAG, "Waiting for system time to sync...");
    if (esp_netif_sntp_sync_wait(pdMS_TO_TICKS(30000)) == ESP_OK) {
        time_t now;
        struct tm timeinfo;
        time(&now);
        localtime_r(&now, &timeinfo);
        ESP_LOGI(TAG, "System time successfully synced! Current Year: %d", (1900 + timeinfo.tm_year));
    } else {
        ESP_LOGE(TAG, "Failed to sync time. Secure TLS connections will likely fail.");
    }
}

static void network_init(void) {
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
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
        .sta = { .ssid = WIFI_SSID, .password = WIFI_PASS },
    };
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());

    EventBits_t bits = xEventGroupWaitBits(s_wifi_event_group, WIFI_CONNECTED_BIT, pdFALSE, pdTRUE, pdMS_TO_TICKS(20000));
    if (bits & WIFI_CONNECTED_BIT) {
        obtain_time();
    }

    esp_mqtt_client_config_t mqtt_cfg = {
        .broker.address.uri = MQTT_BROKER_URI,
        .credentials.username = MQTT_USERNAME,
        .credentials.authentication.password = MQTT_PASSWORD,
        .broker.verification.certificate = mosqmq_root_ca,
    };
    mqtt_client = esp_mqtt_client_init(&mqtt_cfg);
    esp_mqtt_client_register_event(mqtt_client, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
    esp_mqtt_client_start(mqtt_client);
}

// ==========================================
// NON-DESTRUCTIVE SMART I2C SCANNING
// ==========================================
static void scan_i2c_bus(void) {
    uint8_t found_this_run = 0;

    if (xSemaphoreTake(i2c_mutex, pdMS_TO_TICKS(200)) == pdTRUE) {
        if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
            
            for (int i = 0; i < 4; i++) {
                if (bus_handle == NULL) break;
                esp_err_t probe_err = i2c_master_probe(bus_handle, possible_addresses[i], 100);
                
                if (probe_err == ESP_OK) {
                    if (ads_handles[i] == NULL) {
                        i2c_device_config_t dev_cfg = {
                            .dev_addr_length = I2C_ADDR_BIT_LEN_7,
                            .device_address = possible_addresses[i],
                            .scl_speed_hz = I2C_MASTER_FREQ_HZ,
                        };
                        i2c_master_bus_add_device(bus_handle, &dev_cfg, &ads_handles[i]);
                    }
                    global_node_data[i].address = possible_addresses[i];
                    global_node_data[i].is_online = true;
                    found_this_run++;
                } else {
                    if (ads_handles[i] != NULL) {
                        i2c_master_bus_rm_device(ads_handles[i]);
                        ads_handles[i] = NULL;
                    }
                    global_node_data[i].address = possible_addresses[i];
                    global_node_data[i].is_online = false;
                    memset(global_node_data[i].port_values, 0, sizeof(global_node_data[i].port_values));
                }
            }
            num_ads_found = found_this_run;
            xSemaphoreGive(data_mutex);
        }
        xSemaphoreGive(i2c_mutex);
    }
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
// 5. FREERTOS WORKING TASKS
// ==========================================
static uint8_t evaluate_port_status(int16_t raw_value) {
    if (raw_value == -9999) return 3; 
    if (raw_value >= 32760 || raw_value <= -32760) return 2; 
    if (raw_value >= -5 && raw_value <= 5) return 1; 
    return 0; 
}

void ads_reader_task(void *pvParameter) {
    while (1) {
        bool structural_drop_detected = false;

        for (int i = 0; i < 4; i++) {
            if (ads_handles[i] == NULL) continue;

            for (int channel = 0; channel < 4; channel++) {
                if (!port_active[i][channel]) continue;

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

                if (xSemaphoreTake(i2c_mutex, pdMS_TO_TICKS(50)) == pdTRUE) {
                    esp_err_t tx_err = ESP_FAIL;
                    if (ads_handles[i] != NULL) {
                        tx_err = i2c_master_transmit(ads_handles[i], config_data, sizeof(config_data), -1);
                    }
                    xSemaphoreGive(i2c_mutex);

                    if (tx_err != ESP_OK) {
                        if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
                            global_node_data[i].port_values[channel] = -9999;
                            xSemaphoreGive(data_mutex);
                        }
                        structural_drop_detected = true;
                        continue; 
                    }

                    vTaskDelay(pdMS_TO_TICKS(10));

                    if (xSemaphoreTake(i2c_mutex, pdMS_TO_TICKS(50)) == pdTRUE) {
                        esp_err_t rx_err = ESP_FAIL;
                        if (ads_handles[i] != NULL) {
                            rx_err = i2c_master_transmit_receive(ads_handles[i], &reg_pointer, 1, read_buf, sizeof(read_buf), -1);
                        }
                        xSemaphoreGive(i2c_mutex);

                        int16_t final_val = -9999;
                        if (rx_err == ESP_OK) {
                            final_val = (read_buf[0] << 8) | read_buf[1];
                        } else {
                            structural_drop_detected = true;
                        }

                        if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
                            global_node_data[i].port_values[channel] = final_val;
                            xSemaphoreGive(data_mutex);
                        }
                    }
                }
            }
        }

        if (structural_drop_detected) {
            ESP_LOGW(TAG, "Hardware link drop caught! Triggering inline bit-bang recovery routine...");
            recover_i2c_bus();
            xEventGroupSetBits(s_hardware_event_group, I2C_RESCAN_REQUIRED_BIT);
        }

        vTaskDelay(pdMS_TO_TICKS(50));
    }
}

void telemetry_builder_task(void *pvParameter) {
    while (1) {
        if (!is_mqtt_connected) {
            vTaskDelay(pdMS_TO_TICKS(2000)); 
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
            for (int i = 0; i < 4; i++) {
                if (!global_node_data[i].is_online) continue;

                cJSON *chip_obj = cJSON_CreateObject();
                char hex_addr[5];
                sprintf(hex_addr, "0x%02X", global_node_data[i].address);
                cJSON_AddStringToObject(chip_obj, "a", hex_addr);

                cJSON *ports_array = cJSON_AddArrayToObject(chip_obj, "p");
                for (int channel = 0; channel < 4; channel++) {
                    if (port_active[i][channel]) {
                        int16_t raw_reading = global_node_data[i].port_values[channel];
                        uint8_t current_status = evaluate_port_status(raw_reading);

                        int port_data[3] = {channel, raw_reading, current_status};
                        cJSON_AddItemToArray(ports_array, cJSON_CreateIntArray(port_data, 3));
                    }
                }
                cJSON_AddItemToArray(adc_array, chip_obj);
            }
            xSemaphoreGive(data_mutex);
        }

        char *payload_string = cJSON_PrintUnformatted(root);
        if (mqtt_client != NULL && payload_string != NULL) {
            esp_mqtt_client_publish(mqtt_client, MQTT_TOPIC, payload_string, 0, 1, 0);
            ESP_LOGI(TAG, "Telemetry Payload Dispatched: %s", payload_string);
        }
        free(payload_string);
        cJSON_Delete(root);

        vTaskDelay(pdMS_TO_TICKS(10000));
    }
}

static bool is_sensor_attached(int16_t raw_value) {
    if (raw_value == -9999) return false; 
    if (raw_value >= FLOATING_LEAK_MIN && raw_value <= FLOATING_LEAK_MAX) {
        return false; 
    }
    return true; 
}

void discovery_builder_task(void *pvParameter) {
    uint32_t last_detailed_topology = 0xFFFFFFFF; 

    while (1) {
        if (!is_mqtt_connected) {
            vTaskDelay(pdMS_TO_TICKS(1000));
            continue;
        }

        EventBits_t bits = xEventGroupWaitBits(
            s_hardware_event_group,
            I2C_RESCAN_REQUIRED_BIT,
            pdTRUE, 
            pdFALSE,
            pdMS_TO_TICKS(DISCOVERY_INTERVAL_MS)
        );

        if (bits & I2C_RESCAN_REQUIRED_BIT) {
            vTaskDelay(pdMS_TO_TICKS(200)); 
        }

        scan_i2c_bus();

        uint32_t current_detailed_topology = 0x00000000;
        bool local_port_connected_map[4][4] = { {false} };

        if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
            int bit_shift_index = 0;
            
            for (int i = 0; i < 4; i++) {
                if (global_node_data[i].is_online) {
                    current_detailed_topology |= (1 << bit_shift_index);
                }
                bit_shift_index++;

                for (int channel = 0; channel < 4; channel++) {
                    if (global_node_data[i].is_online && port_active[i][channel]) {
                        bool attached = is_sensor_attached(global_node_data[i].port_values[channel]);
                        local_port_connected_map[i][channel] = attached;
                        
                        if (attached) {
                            current_detailed_topology |= (1 << bit_shift_index);
                        }
                    }
                    bit_shift_index++;
                }
            }
            xSemaphoreGive(data_mutex);
        }

        if (current_detailed_topology == last_detailed_topology) {
            continue; 
        }
        
        last_detailed_topology = current_detailed_topology;

        cJSON *root = cJSON_CreateObject();
        cJSON_AddStringToObject(root, "t", "disco");
        cJSON_AddNumberToObject(root, "v", 1);
        cJSON_AddStringToObject(root, "tid", "tenant-123");
        cJSON_AddStringToObject(root, "nid", "N001");
        cJSON_AddNumberToObject(root, "ts", (double)time(NULL));

        cJSON *bus_array = cJSON_AddArrayToObject(root, "buses");
        
        if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
            cJSON_AddNumberToObject(root, "detected_chips", num_ads_found);
            
            for (int i = 0; i < 4; i++) {
                if (global_node_data[i].is_online) {
                    cJSON *chip_obj = cJSON_CreateObject();
                    
                    char hex_addr[5];
                    sprintf(hex_addr, "0x%02X", global_node_data[i].address);
                    cJSON_AddStringToObject(chip_obj, "a", hex_addr);

                    cJSON *ports_obj = cJSON_AddObjectToObject(chip_obj, "ports");
                    for (int channel = 0; channel < 4; channel++) {
                        char port_key[6];
                        sprintf(port_key, "p%d", channel);
                        
                        if (port_active[i][channel]) {
                            cJSON_AddStringToObject(ports_obj, port_key, local_port_connected_map[i][channel] ? "CONNECTED" : "DISCONNECTED");
                        } else {
                            cJSON_AddStringToObject(ports_obj, port_key, "DISABLED");
                        }
                    }
                    cJSON_AddItemToArray(bus_array, chip_obj);
                }
            }
            xSemaphoreGive(data_mutex);
        }

        char *payload_string = cJSON_PrintUnformatted(root);
        if (mqtt_client != NULL && payload_string != NULL) {
            esp_mqtt_client_publish(mqtt_client, MQTT_DISCO_TOPIC, payload_string, 0, 1, 1);
            ESP_LOGW(TAG, "Topology Change Caught! New Discovery Packet Dispatched: %s", payload_string);
        }
        free(payload_string);
        cJSON_Delete(root);
    }
}

// ==========================================
// 6. APP MAIN ENTRY
// ==========================================
void app_main(void) {
    i2c_mutex = xSemaphoreCreateMutex();
    data_mutex = xSemaphoreCreateMutex();
    s_hardware_event_group = xEventGroupCreate();

    init_actuators(); 

    if (i2c_master_init() != ESP_OK) {
        ESP_LOGE(TAG, "Failed to initialize I2C master peripheral engine.");
        vTaskSuspend(NULL);
    }

    scan_i2c_bus();

    xTaskCreate(ads_reader_task, "unified_adc_worker", 3072, NULL, 5, NULL);
    xTaskCreate(telemetry_builder_task, "tlm_json_task", 4096, NULL, 5, NULL);
    xTaskCreate(discovery_builder_task, "disco_json_task", 4096, NULL, 5, NULL);

    network_init();
}