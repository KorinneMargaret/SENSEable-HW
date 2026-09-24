/**
 * Project: Environment-Agnostic IoT Monitoring Framework
 * Architecture: Cellular-First Routing with RTC Data Preservation & Active I2C Recovery
 * Hardware: ESP32-WROOM-32D, SIMCOM A7670C, Mini DS3231 RTC, ADS1115
 * Author: Korinne Margaret V. Sasil, Mikhail Alexi D. Hatulan
 * Institute: University of San Carlos, Talamban Campus
 */

#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <netdb.h>
#include <inttypes.h>
#include <sys/stat.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "freertos/event_groups.h"

#include "esp_system.h"
#include "esp_mac.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_netif.h"
#include "esp_netif_ppp.h"
#include "nvs_flash.h"
#include "nvs.h"
#include "mqtt_client.h"
#include "driver/i2c_master.h"
#include "driver/gpio.h"
#include "driver/ledc.h"
#include "driver/uart.h"
#include "esp_log.h"
#include "cJSON.h"
#include "esp_littlefs.h"
#include "esp_modem_api.h"

#include "credentials.h"   
#include "captive_portal.h"

#include "lwip/dns.h"  
#include "lwip/sockets.h"

static const char *TAG = "THESIS_NODE";

// ==========================================
// TIMING & NETWORK TIMEOUT CONFIGURATIONS
// ==========================================
#define TELEMETRY_INTERVAL_MS     10000
#define DISCOVERY_HEARTBEAT_MS    60000
#define MQTT_NETWORK_TIMEOUT_MS   60000 
#define MQTT_KEEPALIVE_SEC        120
#define MAX_CLOUD_FAILURES        3

#define PROVISION_SWITCH_GPIO     32 
#define SPOOL_FILE_PATH           "/fs/telemetry_spool.jsonl"
#define FLOATING_LEAK_MIN         4500
#define FLOATING_LEAK_MAX         5000

// ==========================================
// DYNAMIC TOPIC & ID BUFFERS
// ==========================================
char node_id[32]; 
char tenant_id[32] = "tenant123"; 
char topic_tlm[128];
char topic_cmd[128];
char topic_ack[128];
char topic_disco[128];
char topic_status[128];

// ==========================================
// EXPANDED ACTUATOR PIN MAPS & MUX LAYOUT
// ==========================================
#define NUM_ACTUATORS       6  
// GPIO 16 & 17 Replaced with 26 & 27 to avoid UART conflict with A7670C
const int actuator_gpios[NUM_ACTUATORS] = {4, 25, 13, 14, 26, 27};
const ledc_channel_t actuator_channels[NUM_ACTUATORS] = {
    LEDC_CHANNEL_0, LEDC_CHANNEL_1, LEDC_CHANNEL_2,
    LEDC_CHANNEL_3, LEDC_CHANNEL_4, LEDC_CHANNEL_5
};
#define ACTUATOR_LEDC_MODE          LEDC_LOW_SPEED_MODE
#define ACTUATOR_LEDC_TIMER         LEDC_TIMER_0
#define ACTUATOR_LEDC_RES           LEDC_TIMER_8_BIT   
#define ACTUATOR_LEDC_FREQ          5000               

typedef struct {
    int target_idx;
    int duration_ms;
    char cid[64];
} auto_shutoff_args_t;

TaskHandle_t auto_shutoff_task_handles[NUM_ACTUATORS] = {NULL};
auto_shutoff_args_t global_timer_args[NUM_ACTUATORS]; 

// ==========================================
// A7670C MODEM UART
// ==========================================
#define MODEM_UART_PORT       UART_NUM_2
#define MODEM_UART_TX_PIN     17
#define MODEM_UART_RX_PIN     16
#define MODEM_BAUD_RATE       115200
#define MODEM_SYNC_RETRIES    30
#define MODEM_SIGNAL_RETRIES  60

// ==========================================
// SYSTEM ENUMS & STATE STORAGE
// ==========================================
typedef enum { HW_MODE_WIFI = 0, HW_MODE_CELLULAR = 1 } HardwareConfig_t;
typedef enum { ROUTE_CLOUD = 0, ROUTE_LOCAL = 1 } RoutingState_t;

static HardwareConfig_t current_hw_mode = HW_MODE_WIFI;
static RoutingState_t current_route_state = ROUTE_CLOUD;
static int cloud_disconnect_count = 0;
volatile bool is_mqtt_connected = false;
volatile bool trigger_failover = false;

char wifi_ssid[64] = "DITO_D825D_2.4"; 
char wifi_pass[64] = "password123";
char cellular_apn[64] = "internet.globe.com.ph"; 
char pri_broker_uri[128] = "mqtts://8f90386c155e4bdcac6e637baf348d96.s1.eu.hivemq.cloud:8883";
char sec_broker_uri[128] = "mqtts://192.168.8.161:8883"; 

// ==========================================
// I2C & HARDWARE GLOBALS
// ==========================================
#define I2C_MASTER_SDA_IO  21
#define I2C_MASTER_SCL_IO  22
#define I2C_MASTER_FREQ_HZ 100000
#define DS3231_ADDR        0x68
#define REG_POINTER_CONVERT 0x00
#define REG_POINTER_CONFIG  0x01

i2c_master_bus_handle_t bus_handle;
i2c_master_dev_handle_t ds3231_handle = NULL;
i2c_master_dev_handle_t ads_handles[4] = {NULL, NULL, NULL, NULL};
const uint8_t possible_addresses[4] = {0x48, 0x49, 0x4A, 0x4B};

uint8_t num_ads_found = 0;

esp_mqtt_client_handle_t mqtt_client = NULL;
static esp_modem_dce_t *modem_dce = NULL;
static esp_netif_t *ppp_netif = NULL;

SemaphoreHandle_t i2c_mutex;
SemaphoreHandle_t data_mutex;
SemaphoreHandle_t task_tracking_mutex;
static EventGroupHandle_t s_network_event_group;
#define PPP_CONNECTED_BIT  BIT0
#define MQTT_CONNECTED_BIT BIT1

static EventGroupHandle_t s_hardware_event_group;
#define I2C_RESCAN_REQUIRED_BIT     BIT0

typedef struct {
    uint8_t address;
    int16_t port_values[4];
    bool is_online;
} NodeData;

NodeData global_node_data[4];
bool port_active[4][4] = {
    {true, true, true, true}, {true, true, true, true},
    {true, true, true, true}, {true, true, true, true}
};

static void configure_mqtt_client(void);
static esp_err_t i2c_master_init(void);

// ==========================================
// DYNAMIC NODE ID GENERATION
// ==========================================
static void init_dynamic_identity(void) {
    uint8_t mac[6];
    esp_read_mac(mac, ESP_MAC_BASE); 
    sprintf(node_id, "NODE-%02X%02X%02X", mac[3], mac[4], mac[5]);
}

static void build_mqtt_topics(void) {
    sprintf(topic_tlm,    "%s/%s/%s/tlm",    MQTT_TOPIC_ROOT, tenant_id, node_id);
    sprintf(topic_cmd,    "%s/%s/%s/cmd",    MQTT_TOPIC_ROOT, tenant_id, node_id);
    sprintf(topic_ack,    "%s/%s/%s/ack",    MQTT_TOPIC_ROOT, tenant_id, node_id);
    sprintf(topic_disco,  "%s/%s/%s/disco",  MQTT_TOPIC_ROOT, tenant_id, node_id);
    sprintf(topic_status, "%s/%s/%s/status", MQTT_TOPIC_ROOT, tenant_id, node_id);

    ESP_LOGI(TAG, "====================================");
    ESP_LOGI(TAG, "PROVISIONED AS: %s / %s", tenant_id, node_id);
    ESP_LOGI(TAG, "Command Topic: %s", topic_cmd);
    ESP_LOGI(TAG, "Telemetry Topic: %s", topic_tlm);
    ESP_LOGI(TAG, "====================================");
}

// ==========================================
// DS3231 RTC TIMEKEEPING
// ==========================================
static uint8_t bcd2dec(uint8_t val) { return ((val / 16 * 10) + (val % 16)); }

static time_t get_rtc_epoch(void) {
    if (ds3231_handle == NULL) return time(NULL);

    uint8_t reg = 0x00;
    uint8_t data[7];
    time_t epoch = 0;

    if (xSemaphoreTake(i2c_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        if (i2c_master_transmit_receive(ds3231_handle, &reg, 1, data, 7, 100) == ESP_OK) {
            struct tm t;
            t.tm_sec  = bcd2dec(data[0] & 0x7F);
            t.tm_min  = bcd2dec(data[1]);
            t.tm_hour = bcd2dec(data[2] & 0x3F);
            t.tm_mday = bcd2dec(data[4]);
            t.tm_mon  = bcd2dec(data[5] & 0x1F) - 1;
            t.tm_year = bcd2dec(data[6]) + 100;
            t.tm_isdst = 0;
            epoch = mktime(&t);
        }
        xSemaphoreGive(i2c_mutex);
    }
    return (epoch > 0) ? epoch : time(NULL);
}

// ==========================================
// TWO-STEP ACKNOWLEDGEMENT LOGIC
// ==========================================
static void send_command_ack(const char *cid, const char *status, const char *details) {
    if (!is_mqtt_connected || mqtt_client == NULL) return;

    cJSON *ack_root = cJSON_CreateObject();
    if (ack_root == NULL) return;

    cJSON_AddStringToObject(ack_root, "t", "ack");
    cJSON_AddNumberToObject(ack_root, "v", 1);
    cJSON_AddStringToObject(ack_root, "tid", tenant_id);
    cJSON_AddStringToObject(ack_root, "nid", node_id);
    cJSON_AddStringToObject(ack_root, "cid", cid ? cid : "unknown");
    cJSON_AddStringToObject(ack_root, "status", status);
    cJSON_AddStringToObject(ack_root, "details", details ? details : "");
    cJSON_AddNumberToObject(ack_root, "ts", (double)get_rtc_epoch());

    char *payload = cJSON_PrintUnformatted(ack_root);
    if (payload != NULL) {
        esp_mqtt_client_publish(mqtt_client, topic_ack, payload, 0, 1, 0);
        ESP_LOGI(TAG, "Command ACK published -> Status: %s | ID: %s", status, cid ? cid : "unknown");
        free(payload);
    }
    cJSON_Delete(ack_root);
}

// ==========================================
// HARDWARE INITIALIZATION FOR ACTUATORS
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

void auto_shutoff_task(void *pvParameter) {
    auto_shutoff_args_t *args = (auto_shutoff_args_t *)pvParameter;
    vTaskDelay(pdMS_TO_TICKS(args->duration_ms));
    
    int mapped_gpio = actuator_gpios[args->target_idx];
    ledc_channel_t mapped_chan = actuator_channels[args->target_idx];
    ledc_stop(ACTUATOR_LEDC_MODE, mapped_chan, 0);
    gpio_set_level(mapped_gpio, 0);
    
    ESP_LOGW(TAG, ">>> Auto-shutoff triggered for OUT%d after %d ms <<<", args->target_idx + 1, args->duration_ms);
    send_command_ack(args->cid, "completed", "Auto-shutoff execution window expired safely");

    if (xSemaphoreTake(task_tracking_mutex, portMAX_DELAY) == pdTRUE) {
        auto_shutoff_task_handles[args->target_idx] = NULL;
        xSemaphoreGive(task_tracking_mutex);
    }
    vTaskDelete(NULL);
}

// ==========================================
// LITTLEFS OFFLINE STORAGE
// ==========================================
static void init_littlefs(void) {
    esp_vfs_littlefs_conf_t conf = {
        .base_path = "/fs", .partition_label = "spiffs", .format_if_mount_failed = true, .dont_mount = false,
    };
    if (esp_vfs_littlefs_register(&conf) == ESP_OK) ESP_LOGI(TAG, "LittleFS mounted for WAN offline buffering.");
}

// ==========================================
// BIT-BANG I2C BUS RECOVERY ROUTINE
// ==========================================
static void recover_i2c_bus(void) {
    ESP_LOGW(TAG, "Executing structured software bit-bang recovery routine...");

    if (xSemaphoreTake(i2c_mutex, portMAX_DELAY) == pdTRUE) {
        for (int i = 0; i < 4; i++) {
            if (ads_handles[i] != NULL) { i2c_master_bus_rm_device(ads_handles[i]); ads_handles[i] = NULL; }
        }
        if (ds3231_handle != NULL) { i2c_master_bus_rm_device(ds3231_handle); ds3231_handle = NULL; }
        if (bus_handle != NULL) { i2c_del_master_bus(bus_handle); bus_handle = NULL; }

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
            gpio_set_level(I2C_MASTER_SCL_IO, 0); vTaskDelay(pdMS_TO_TICKS(5));
            gpio_set_level(I2C_MASTER_SCL_IO, 1); vTaskDelay(pdMS_TO_TICKS(5));
        }

        gpio_set_level(I2C_MASTER_SDA_IO, 0); vTaskDelay(pdMS_TO_TICKS(5));
        gpio_set_level(I2C_MASTER_SCL_IO, 1); vTaskDelay(pdMS_TO_TICKS(5));
        gpio_set_level(I2C_MASTER_SDA_IO, 1); vTaskDelay(pdMS_TO_TICKS(5));

        // ---> ADD THESE TWO LINES HERE <---
        gpio_reset_pin(I2C_MASTER_SDA_IO);
        gpio_reset_pin(I2C_MASTER_SCL_IO);

        if (i2c_master_init() == ESP_OK) {
            i2c_device_config_t ds_cfg = { .dev_addr_length = I2C_ADDR_BIT_LEN_7, .device_address = DS3231_ADDR, .scl_speed_hz = I2C_MASTER_FREQ_HZ };
            i2c_master_bus_add_device(bus_handle, &ds_cfg, &ds3231_handle);
            ESP_LOGI(TAG, "Hardware core I2C registers and RTC restored.");
        } else {
            ESP_LOGE(TAG, "Fatal fault re-instantiating core hardware I2C master bus.");
        }
        xSemaphoreGive(i2c_mutex);
    }
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
                        i2c_device_config_t dev_cfg = { .dev_addr_length = I2C_ADDR_BIT_LEN_7, .device_address = possible_addresses[i], .scl_speed_hz = I2C_MASTER_FREQ_HZ };
                        i2c_master_bus_add_device(bus_handle, &dev_cfg, &ads_handles[i]);
                    }
                    global_node_data[i].address = possible_addresses[i]; global_node_data[i].is_online = true; found_this_run++;
                } else {
                    if (ads_handles[i] != NULL) { i2c_master_bus_rm_device(ads_handles[i]); ads_handles[i] = NULL; }
                    global_node_data[i].address = possible_addresses[i]; global_node_data[i].is_online = false;
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
        .clk_source = I2C_CLK_SRC_DEFAULT, .i2c_port = -1, .scl_io_num = I2C_MASTER_SCL_IO, .sda_io_num = I2C_MASTER_SDA_IO,
        .glitch_ignore_cnt = 7, .flags.enable_internal_pullup = true,
    };
    return i2c_new_master_bus(&i2c_mst_config, &bus_handle);
}

// ==========================================
// ACTIVE RESPONSIVE ADS1115 POLLING TASK
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
            if (ads_handles[i] == NULL || !global_node_data[i].is_online) continue;

            for (int channel = 0; channel < 4; channel++) {
                if (!port_active[i][channel]) continue;

                uint8_t config_msb;
                switch (channel) {
                    case 0: config_msb = 0xC3; break; case 1: config_msb = 0xD3; break;
                    case 2: config_msb = 0xE3; break; case 3: config_msb = 0xF3; break;
                }
                uint8_t config_data[3] = {REG_POINTER_CONFIG, config_msb, 0x83};
                uint8_t reg_pointer = REG_POINTER_CONVERT; uint8_t read_buf[2];

                if (xSemaphoreTake(i2c_mutex, pdMS_TO_TICKS(50)) == pdTRUE) {
                    esp_err_t tx_err = ESP_FAIL;
                    if (ads_handles[i] != NULL) tx_err = i2c_master_transmit(ads_handles[i], config_data, sizeof(config_data), 100);
                    xSemaphoreGive(i2c_mutex);

                    if (tx_err != ESP_OK) {
                        if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
                            global_node_data[i].port_values[channel] = -9999; xSemaphoreGive(data_mutex);
                        }
                        structural_drop_detected = true; continue; 
                    }
                    vTaskDelay(pdMS_TO_TICKS(10));

                    if (xSemaphoreTake(i2c_mutex, pdMS_TO_TICKS(50)) == pdTRUE) {
                        esp_err_t rx_err = ESP_FAIL;
                        if (ads_handles[i] != NULL) rx_err = i2c_master_transmit_receive(ads_handles[i], &reg_pointer, 1, read_buf, sizeof(read_buf), 100);
                        xSemaphoreGive(i2c_mutex);

                        int16_t final_val = -9999;
                        if (rx_err == ESP_OK) final_val = (read_buf[0] << 8) | read_buf[1];
                        else structural_drop_detected = true;

                        if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
                            global_node_data[i].port_values[channel] = final_val; xSemaphoreGive(data_mutex);
                        }
                    }
                }
            }
        }
        if (structural_drop_detected) {
            ESP_LOGW(TAG, "Hardware link drop caught! Triggering inline bit-bang recovery routine...");
            recover_i2c_bus(); xEventGroupSetBits(s_hardware_event_group, I2C_RESCAN_REQUIRED_BIT);
        }
        vTaskDelay(pdMS_TO_TICKS(50));
    }
}

// ==========================================
// INCOMING COMMAND PARSER
// ==========================================
static void process_incoming_command(const char *json_string) {
    cJSON *root = cJSON_Parse(json_string);
    if (!root) {
        ESP_LOGE(TAG, "Failed to parse incoming command payload as valid JSON.");
        return;
    }

    cJSON *cid_item = cJSON_GetObjectItem(root, "cid");
    const char *cid_str = (cid_item && cJSON_IsString(cid_item)) ? cid_item->valuestring : NULL;

    if (!cid_str) {
        ESP_LOGW(TAG, "Command rejected: missing cid");
        cJSON_Delete(root);
        return; 
    }

    cJSON *act_item = cJSON_GetObjectItem(root, "action");
    if (act_item && cJSON_IsString(act_item)) {
        const char *action = act_item->valuestring;

        if (strcmp(action, "bus_recovery") == 0) {
            cJSON *bus_id_item = cJSON_GetObjectItem(root, "bus_id");
            int target_bus = (bus_id_item && cJSON_IsNumber(bus_id_item)) ? bus_id_item->valueint : 0;
            ESP_LOGI(TAG, "Schema payload validated for Bus %d. Executing recovery routine...", target_bus);

            send_command_ack(cid_str, "started", "Beginning I2C recovery cycle");
            recover_i2c_bus(); xEventGroupSetBits(s_hardware_event_group, I2C_RESCAN_REQUIRED_BIT);
            send_command_ack(cid_str, "completed", "I2C bus recovery complete");
        }
        else if (strcmp(action, "actuate") == 0) {
            cJSON *port_item = cJSON_GetObjectItem(root, "port");
            cJSON *mode_item = cJSON_GetObjectItem(root, "mode");
            cJSON *dur_item  = cJSON_GetObjectItem(root, "dur"); 

            if (port_item && mode_item && dur_item && cJSON_IsNumber(port_item) && cJSON_IsString(mode_item)) {
                int target_idx = port_item->valueint - 1; 
                const char *mode_str = mode_item->valuestring;
                int duration_ms = cJSON_IsNumber(dur_item) ? dur_item->valueint : 0;

                if (target_idx >= 0 && target_idx < NUM_ACTUATORS) { 
                    int mapped_gpio = actuator_gpios[target_idx];
                    ledc_channel_t mapped_chan = actuator_channels[target_idx];
                    bool actuator_active_state = false;

                    if (strcmp(mode_str, "bin") == 0) {
                        cJSON *state_item = cJSON_GetObjectItem(root, "state");
                        if (state_item && cJSON_IsNumber(state_item)) {
                            int state_val = state_item->valueint; actuator_active_state = (state_val > 0);
                            ledc_stop(ACTUATOR_LEDC_MODE, mapped_chan, state_val); gpio_set_level(mapped_gpio, state_val);
                            ESP_LOGI(TAG, "Binary: OUT%d (GPIO %d) -> %d [Duration: %d ms]", target_idx + 1, mapped_gpio, state_val, duration_ms);
                        }
                    } 
                    else if (strcmp(mode_str, "pwm") == 0) {
                        cJSON *state_item = cJSON_GetObjectItem(root, "state");
                        cJSON *duty_item  = cJSON_GetObjectItem(root, "duty");

                        if (state_item && cJSON_IsNumber(state_item) && state_item->valueint == 0) {
                            ledc_set_duty(ACTUATOR_LEDC_MODE, mapped_chan, 0); ledc_update_duty(ACTUATOR_LEDC_MODE, mapped_chan);
                            actuator_active_state = false;
                            ESP_LOGI(TAG, "PWM: OUT%d -> STOP (explicit state:0)", target_idx + 1);
                        }
                        else if (duty_item && cJSON_IsNumber(duty_item)) {
                            int duty_val = duty_item->valueint; actuator_active_state = (duty_val > 0);
                            ledc_set_duty(ACTUATOR_LEDC_MODE, mapped_chan, duty_val); ledc_update_duty(ACTUATOR_LEDC_MODE, mapped_chan);
                            ESP_LOGI(TAG, "PWM: OUT%d (GPIO %d) -> Duty: %d/255 [Duration: %d ms]", target_idx + 1, mapped_gpio, duty_val, duration_ms);
                        }
                    }

                    if (xSemaphoreTake(task_tracking_mutex, portMAX_DELAY) == pdTRUE) {
                        if (auto_shutoff_task_handles[target_idx] != NULL) {
                            vTaskDelete(auto_shutoff_task_handles[target_idx]); auto_shutoff_task_handles[target_idx] = NULL;
                            ESP_LOGW(TAG, "Safely terminated legacy timed worker task on OUT%d to protect override context.", target_idx + 1);
                        }
                        xSemaphoreGive(task_tracking_mutex);
                    }

                    if (actuator_active_state) {
                        if (duration_ms > 0) {
                            send_command_ack(cid_str, "started", "Actuator driven high, auto-shutoff armed");
                            global_timer_args[target_idx].target_idx = target_idx;
                            global_timer_args[target_idx].duration_ms = duration_ms;
                            strncpy(global_timer_args[target_idx].cid, cid_str, sizeof(global_timer_args[target_idx].cid) - 1);
                            global_timer_args[target_idx].cid[sizeof(global_timer_args[target_idx].cid) - 1] = '\0';
                            
                            if (xSemaphoreTake(task_tracking_mutex, portMAX_DELAY) == pdTRUE) {
                                xTaskCreate(auto_shutoff_task, "auto_shutoff", 2048, (void *)&global_timer_args[target_idx], 5, &auto_shutoff_task_handles[target_idx]);
                                xSemaphoreGive(task_tracking_mutex);
                            }
                        } else { send_command_ack(cid_str, "started", "Actuator driven high indefinitely"); }
                    } else { send_command_ack(cid_str, "stopped", "Actuator set to default idle state"); }
                    
                } else { send_command_ack(cid_str, "failed", "Port limit out of bounds"); }
            } else { send_command_ack(cid_str, "failed", "Missing dynamic execution parameters"); }
        }
        else if (strcmp(action, "sensor_port_up") == 0 || strcmp(action, "sensor_port_down") == 0) {
            cJSON *chip_item = cJSON_GetObjectItem(root, "chip");
            cJSON *ch_item   = cJSON_GetObjectItem(root, "ch");
            
            if (chip_item && ch_item && cJSON_IsNumber(chip_item) && cJSON_IsNumber(ch_item)) {
                int chip_idx = chip_item->valueint; int ch_idx   = ch_item->valueint;
                
                if (chip_idx >= 0 && chip_idx < 4 && ch_idx >= 0 && ch_idx < 4) {
                    bool set_active = (strcmp(action, "sensor_port_up") == 0);
                    send_command_ack(cid_str, "started", "Modifying software configuration states");
                    
                    if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
                        port_active[chip_idx][ch_idx] = set_active; xSemaphoreGive(data_mutex);
                    }
                    ESP_LOGW(TAG, "Altered configuration: Chip Index [%d] Port [%d] -> %s", chip_idx, ch_idx, set_active ? "ENABLED" : "DISABLED");
                    xEventGroupSetBits(s_hardware_event_group, I2C_RESCAN_REQUIRED_BIT);
                    send_command_ack(cid_str, "completed", "Target port map dynamically adjusted");
                } else { send_command_ack(cid_str, "failed", "Chip or port argument range out of bounds"); }
            }
        }
    }
    cJSON_Delete(root);
}

// ==========================================
// MQTT & TOPOLOGY-AWARE FAILOVER LOGIC
// ==========================================
static void mqtt_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data) {
    esp_mqtt_event_handle_t event = event_data;

    if (event_id == MQTT_EVENT_CONNECTED) {
        ESP_LOGI(TAG, "MQTT Connected! Route: %s", current_route_state == ROUTE_CLOUD ? "CLOUD" : "LOCAL");
        is_mqtt_connected = true; cloud_disconnect_count = 0;
        xEventGroupSetBits(s_network_event_group, MQTT_CONNECTED_BIT);
        xEventGroupSetBits(s_hardware_event_group, I2C_RESCAN_REQUIRED_BIT); 

        esp_mqtt_client_subscribe(mqtt_client, topic_cmd, 1);
        ESP_LOGI(TAG, "Subscribed to Command Topic: %s", topic_cmd);

        esp_mqtt_client_publish(mqtt_client, topic_status, "{\"t\":\"lwt\",\"status\":\"online\"}", 0, 1, 1);
    }
    else if (event_id == MQTT_EVENT_DISCONNECTED) {
        ESP_LOGW(TAG, "MQTT Disconnected.");
        is_mqtt_connected = false; xEventGroupClearBits(s_network_event_group, MQTT_CONNECTED_BIT);

        if (current_hw_mode == HW_MODE_WIFI && current_route_state == ROUTE_CLOUD) {
            cloud_disconnect_count++;
            ESP_LOGW(TAG, "Cloud connection attempt %d failed.", cloud_disconnect_count);
            if (cloud_disconnect_count >= MAX_CLOUD_FAILURES) {
                ESP_LOGE(TAG, "Cloud unreachable. Flagging for safe Phase 2 Failover...");
                trigger_failover = true;
            }
        }
    }
    else if (event_id == MQTT_EVENT_DATA) {
        if (event->data_len >= 1024) {
            ESP_LOGE(TAG, "Payload exceeds stack buffer size. Dropping packet.");
            return;
        }
        char json_string[1024]; memcpy(json_string, event->data, event->data_len); json_string[event->data_len] = '\0';
        ESP_LOGI(TAG, "Command Packet Received on [%.*s]: %s", event->topic_len, event->topic, json_string);
        process_incoming_command(json_string);
    }
}

static void configure_mqtt_client(void) {
    if (mqtt_client != NULL) { esp_mqtt_client_stop(mqtt_client); esp_mqtt_client_destroy(mqtt_client); }

    esp_mqtt_client_config_t mqtt_cfg = {0};
    mqtt_cfg.network.timeout_ms = MQTT_NETWORK_TIMEOUT_MS; 
    mqtt_cfg.session.last_will.topic   = topic_status;
    mqtt_cfg.session.last_will.msg     = "{\"t\":\"lwt\",\"status\":\"offline\"}";
    mqtt_cfg.session.last_will.qos     = 1;
    mqtt_cfg.session.last_will.retain  = 1;
    mqtt_cfg.session.keepalive        = MQTT_KEEPALIVE_SEC;

    if (current_route_state == ROUTE_CLOUD) {
        mqtt_cfg.broker.address.uri = pri_broker_uri;
        if (strncmp(pri_broker_uri, "mqtts://", strlen("mqtts://")) == 0) {
            mqtt_cfg.broker.verification.certificate = mosqmq_root_ca;
            mqtt_cfg.credentials.username = MQTT_USERNAME;
            mqtt_cfg.credentials.authentication.password = MQTT_PASSWORD;
        }
    } else if (current_route_state == ROUTE_LOCAL) {
        mqtt_cfg.broker.address.uri = sec_broker_uri; 
        if (strncmp(sec_broker_uri, "mqtts://", strlen("mqtts://")) == 0) {
            mqtt_cfg.broker.verification.certificate = mosqmq_root_ca;
            mqtt_cfg.credentials.username = LOCAL_MQTT_USERNAME;
            mqtt_cfg.credentials.authentication.password = LOCAL_MQTT_PASSWORD;
        }
    }
    mqtt_client = esp_mqtt_client_init(&mqtt_cfg);
    esp_mqtt_client_register_event(mqtt_client, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
    esp_mqtt_client_start(mqtt_client);
}

// ==========================================
// BULLETPROOF WAN CONNECTIVITY PROBE
// ==========================================
static bool is_internet_available(void) {
    struct addrinfo hints = { .ai_family = AF_INET, .ai_socktype = SOCK_STREAM };
    struct addrinfo *res;
    int err = getaddrinfo("google.com", "80", &hints, &res);
    if (err == 0) {
        int sock = socket(res->ai_family, res->ai_socktype, 0);
        if (sock >= 0) {
            struct timeval to = { .tv_sec = 3, .tv_usec = 0 };
            setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &to, sizeof(to)); setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &to, sizeof(to));
            int conn_err = connect(sock, res->ai_addr, res->ai_addrlen);
            close(sock); freeaddrinfo(res);
            if (conn_err == 0) return true; 
        } else { freeaddrinfo(res); }
    }
    return false; 
}

// ==========================================
// BACKGROUND CLOUD RECOVERY WATCHDOG
// ==========================================
void cloud_watchdog_task(void *pvParameter) {
    TickType_t last_cloud_ping = xTaskGetTickCount();

    while (1) {
        if (trigger_failover) {
            trigger_failover = false; 
            ESP_LOGW(TAG, "Executing Phase 2: Local Edge Server Routing...");
            current_route_state = ROUTE_LOCAL; configure_mqtt_client();
        }

        if (current_hw_mode == HW_MODE_WIFI && current_route_state == ROUTE_LOCAL) {
            if ((xTaskGetTickCount() - last_cloud_ping) > pdMS_TO_TICKS(60000)) {
                last_cloud_ping = xTaskGetTickCount();
                ESP_LOGI(TAG, "Watchdog probing WAN connectivity silently...");
                
                if (is_internet_available()) {
                    ESP_LOGI(TAG, "Internet restored! Tearing down local socket and returning to Cloud Phase 1...");
                    current_route_state = ROUTE_CLOUD; cloud_disconnect_count = 0; configure_mqtt_client(); 
                } else {
                    ESP_LOGW(TAG, "Internet still down. Preserving Phase 2 local edge routing.");
                }
            }
        }
        vTaskDelay(pdMS_TO_TICKS(1000));
    }
}

// ==========================================
// CELLULAR (A7670C over PPP)
// ==========================================
static void ppp_ip_event_handler(void *arg, esp_event_base_t base, int32_t event_id, void *event_data) {
    if (event_id == IP_EVENT_PPP_GOT_IP) {
        ip_event_got_ip_t *event = (ip_event_got_ip_t *)event_data;
        ESP_LOGI(TAG, "Cellular PPP up. IP: " IPSTR, IP2STR(&event->ip_info.ip));
        esp_netif_dns_info_t dns_info = {0}; dns_info.ip.type = ESP_IPADDR_TYPE_V4;
        esp_netif_str_to_ip4("8.8.8.8", &dns_info.ip.u_addr.ip4);
        esp_netif_set_dns_info(ppp_netif, ESP_NETIF_DNS_MAIN, &dns_info);
        ESP_LOGI(TAG, "Forced Google DNS (8.8.8.8) to bypass carrier DNS failure.");
        xEventGroupSetBits(s_network_event_group, PPP_CONNECTED_BIT);
    } else if (event_id == IP_EVENT_PPP_LOST_IP) {
        ESP_LOGW(TAG, "Cellular PPP lost IP.");
        xEventGroupClearBits(s_network_event_group, PPP_CONNECTED_BIT);
    }
}

static bool modem_bring_up(void) {
    esp_modem_set_mode(modem_dce, ESP_MODEM_MODE_COMMAND);
    int tries = 0;
    while (esp_modem_sync(modem_dce) != ESP_OK) {
        if (++tries >= MODEM_SYNC_RETRIES) {
            ESP_LOGE(TAG, "Modem not responding on UART. Check TX/RX swap, GND, modem power.");
            return false;
        }
        vTaskDelay(pdMS_TO_TICKS(1000));
    }
    ESP_LOGI(TAG, "Modem responded to AT.");
    bool pin_ok = false;
    if (esp_modem_read_pin(modem_dce, &pin_ok) != ESP_OK || !pin_ok) {
        ESP_LOGE(TAG, "SIM not ready (missing, locked with PIN, or bad contact).");
        return false;
    }
    ESP_LOGI(TAG, "SIM ready.");

    int rssi = 99, ber = 99; tries = 0;
    while (1) {
        if (esp_modem_get_signal_quality(modem_dce, &rssi, &ber) == ESP_OK && rssi > 0 && rssi != 99) break;
        if (++tries >= MODEM_SIGNAL_RETRIES) {
            ESP_LOGE(TAG, "No cellular signal (CSQ=%d). Check antenna and coverage.", rssi);
            return false;
        }
        vTaskDelay(pdMS_TO_TICKS(1000));
    }
    ESP_LOGI(TAG, "Signal OK. CSQ=%d (~%d dBm), BER=%d", rssi, -113 + 2 * rssi, ber);

    if (esp_modem_set_mode(modem_dce, ESP_MODEM_MODE_DATA) != ESP_OK) {
        ESP_LOGE(TAG, "Failed to enter PPP data mode (check APN: %s).", cellular_apn);
        return false;
    }
    ESP_LOGI(TAG, "PPP data mode requested. Waiting for IP...");
    return true;
}

void cellular_task(void *pvParameter) {
    esp_event_handler_register(IP_EVENT, IP_EVENT_PPP_GOT_IP, ppp_ip_event_handler, NULL);
    esp_event_handler_register(IP_EVENT, IP_EVENT_PPP_LOST_IP, ppp_ip_event_handler, NULL);

    esp_modem_dte_config_t dte_config = ESP_MODEM_DTE_DEFAULT_CONFIG();
    dte_config.uart_config.port_num = MODEM_UART_PORT; dte_config.uart_config.tx_io_num = MODEM_UART_TX_PIN;
    dte_config.uart_config.rx_io_num = MODEM_UART_RX_PIN; dte_config.uart_config.baud_rate = MODEM_BAUD_RATE;
    
    esp_modem_dce_config_t dce_config = ESP_MODEM_DCE_DEFAULT_CONFIG(cellular_apn);
    esp_netif_config_t netif_ppp_config = ESP_NETIF_DEFAULT_PPP();
    ppp_netif = esp_netif_new(&netif_ppp_config);

#ifdef CONFIG_LWIP_PPP_PAP_SUPPORT
    if (strlen(CELLULAR_USER) > 0) esp_netif_ppp_set_auth(ppp_netif, NETIF_PPP_AUTHTYPE_PAP, CELLULAR_USER, CELLULAR_PASS);
#endif

    modem_dce = esp_modem_new_dev(ESP_MODEM_DCE_GENERIC, &dte_config, &dce_config, ppp_netif);
    if (modem_dce == NULL) { 
        ESP_LOGE(TAG, "esp_modem_new_dev failed.");
        vTaskDelete(NULL); 
        return; 
    }

    while (1) {
        while (!modem_bring_up()) {
            ESP_LOGW(TAG, "Retrying modem bring-up in 15 s...");
            vTaskDelay(pdMS_TO_TICKS(15000));
        }
        EventBits_t bits = xEventGroupWaitBits(s_network_event_group, PPP_CONNECTED_BIT, pdFALSE, pdTRUE, pdMS_TO_TICKS(60000));
        if (!(bits & PPP_CONNECTED_BIT)) {
            ESP_LOGE(TAG, "PPP did not get an IP within 60 s. Restarting modem sequence.");
            continue;
        }
        
        if (mqtt_client == NULL) { current_route_state = ROUTE_CLOUD; configure_mqtt_client(); }

        int down_seconds = 0;
        while (down_seconds < 30) {
            if (xEventGroupGetBits(s_network_event_group) & PPP_CONNECTED_BIT) down_seconds = 0; else down_seconds++;
            vTaskDelay(pdMS_TO_TICKS(1000));
        }
        ESP_LOGW(TAG, "PPP down for 30 s. Re-initializing modem.");
    }
}

// ==========================================
// TELEMETRY BUILDER (THE ROUTER)
// ==========================================
void telemetry_builder_task(void *pvParameter) {
    while (1) {
        cJSON *root = cJSON_CreateObject();
        cJSON_AddStringToObject(root, "t", "tlm"); cJSON_AddNumberToObject(root, "v", 1);
        cJSON_AddStringToObject(root, "tid", tenant_id); cJSON_AddStringToObject(root, "nid", node_id);
        cJSON_AddStringToObject(root, "net", (current_hw_mode == HW_MODE_CELLULAR) ? "cell" : "wifi");
        cJSON_AddNumberToObject(root, "ts", (double)get_rtc_epoch());

        cJSON *adc_array = cJSON_AddArrayToObject(root, "adc");

        if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
            for (int i = 0; i < 4; i++) {
                if (!global_node_data[i].is_online) continue;
                cJSON *chip_obj = cJSON_CreateObject();
                char hex_addr[5]; sprintf(hex_addr, "0x%02X", global_node_data[i].address);
                cJSON_AddStringToObject(chip_obj, "a", hex_addr);

                cJSON *ports_array = cJSON_AddArrayToObject(chip_obj, "p");
                for (int channel = 0; channel < 4; channel++) {
                    if (port_active[i][channel]) {
                        int16_t raw_reading = global_node_data[i].port_values[channel];
                        int port_data[3] = {channel, raw_reading, evaluate_port_status(raw_reading)}; 
                        cJSON_AddItemToArray(ports_array, cJSON_CreateIntArray(port_data, 3));
                    }
                }
                cJSON_AddItemToArray(adc_array, chip_obj);
            }
            xSemaphoreGive(data_mutex);
        }

        if (is_mqtt_connected && mqtt_client != NULL) {
            char *payload_string = cJSON_PrintUnformatted(root);
            esp_mqtt_client_publish(mqtt_client, topic_tlm, payload_string, 0, 1, 0);
            ESP_LOGI(TAG, "Live Payload Dispatched -> %s", payload_string);
            free(payload_string);
        } else if (current_hw_mode == HW_MODE_CELLULAR) {
            char *buffered_string = cJSON_PrintUnformatted(root);
            FILE *f = fopen(SPOOL_FILE_PATH, "a");
            if (f != NULL) { 
                fprintf(f, "%s\n", buffered_string); 
                fclose(f); 
                ESP_LOGW(TAG, "[CELLULAR DROP] Buffered to LittleFS -> %s", buffered_string);
            } else {
                ESP_LOGE(TAG, "Spool open failed (%s). Telemetry dropped.", SPOOL_FILE_PATH);
            }
            free(buffered_string);
        }
        cJSON_Delete(root);
        vTaskDelay(pdMS_TO_TICKS(TELEMETRY_INTERVAL_MS));
    }
}

// ==========================================
// DISCOVERY BUILDER TASK
// ==========================================
static bool is_sensor_attached(int16_t raw_value) {
    if (raw_value == -9999) return false; 
    if (raw_value >= FLOATING_LEAK_MIN && raw_value <= FLOATING_LEAK_MAX) {
        return false; 
    }
    return true; 
}

void discovery_builder_task(void *pvParameter) {
    uint32_t last_detailed_topology = 0xFFFFFFFF; static TickType_t last_disco_publish = 0;

    while (1) {
        if (!is_mqtt_connected) { vTaskDelay(pdMS_TO_TICKS(1000)); continue; }

        EventBits_t bits = xEventGroupWaitBits(s_hardware_event_group, I2C_RESCAN_REQUIRED_BIT, pdTRUE, pdFALSE, pdMS_TO_TICKS(DISCOVERY_INTERVAL_MS));
        if (bits & I2C_RESCAN_REQUIRED_BIT) vTaskDelay(pdMS_TO_TICKS(200)); 

        scan_i2c_bus();
        uint32_t current_detailed_topology = 0x00000000;
        bool local_port_connected_map[4][4] = { {false} };

        if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
            int bit_shift_index = 0;
            for (int i = 0; i < 4; i++) {
                if (global_node_data[i].is_online) current_detailed_topology |= (1 << bit_shift_index);
                bit_shift_index++;
                for (int channel = 0; channel < 4; channel++) {
                    if (global_node_data[i].is_online && port_active[i][channel]) {
                        bool attached = is_sensor_attached(global_node_data[i].port_values[channel]);
                        local_port_connected_map[i][channel] = attached;
                        if (attached) current_detailed_topology |= (1 << bit_shift_index);
                    }
                    bit_shift_index++;
                }
            }
            xSemaphoreGive(data_mutex);
        }

        bool topology_changed = (current_detailed_topology != last_detailed_topology);
        bool heartbeat_due = (xTaskGetTickCount() - last_disco_publish) > pdMS_TO_TICKS(DISCOVERY_HEARTBEAT_MS);

        if (!topology_changed && !heartbeat_due) continue; 
        
        last_detailed_topology = current_detailed_topology; last_disco_publish = xTaskGetTickCount();

        cJSON *root = cJSON_CreateObject();
        cJSON_AddStringToObject(root, "t", "disco"); cJSON_AddNumberToObject(root, "v", 1);
        cJSON_AddStringToObject(root, "tid", tenant_id); cJSON_AddStringToObject(root, "nid", node_id);
        cJSON_AddStringToObject(root, "net", (current_hw_mode == HW_MODE_CELLULAR) ? "cell" : "wifi");
        cJSON_AddNumberToObject(root, "ts", (double)get_rtc_epoch());
        cJSON_AddNumberToObject(root, "tlm_interval_ms", TELEMETRY_INTERVAL_MS);

        cJSON *bus_array = cJSON_AddArrayToObject(root, "buses");
        if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
            cJSON_AddNumberToObject(root, "detected_chips", num_ads_found);
            for (int i = 0; i < 4; i++) {
                if (global_node_data[i].is_online) {
                    cJSON *chip_obj = cJSON_CreateObject();
                    char hex_addr[5]; sprintf(hex_addr, "0x%02X", global_node_data[i].address);
                    cJSON_AddStringToObject(chip_obj, "a", hex_addr);

                    cJSON *ports_obj = cJSON_AddObjectToObject(chip_obj, "ports");
                    for (int channel = 0; channel < 4; channel++) {
                        char port_key[6]; sprintf(port_key, "p%d", channel);
                        if (port_active[i][channel]) cJSON_AddStringToObject(ports_obj, port_key, local_port_connected_map[i][channel] ? "CONNECTED" : "DISCONNECTED");
                        else cJSON_AddStringToObject(ports_obj, port_key, "DISABLED");
                    }
                    cJSON_AddItemToArray(bus_array, chip_obj);
                }
            }
            xSemaphoreGive(data_mutex);
        }
        char *payload_string = cJSON_PrintUnformatted(root);
        if (mqtt_client != NULL && payload_string != NULL) {
            esp_mqtt_client_publish(mqtt_client, topic_disco, payload_string, 0, 1, 1);
            ESP_LOGW(TAG, "Topology Change Caught! New Discovery Packet Dispatched: %s", payload_string);
        }
        free(payload_string); cJSON_Delete(root);
    }
}

// ==========================================
// CELLULAR FIFO REPLAY
// ==========================================
void backlog_replay_task(void *pvParameter) {
    while (1) {
        if (current_hw_mode != HW_MODE_CELLULAR) { vTaskDelay(pdMS_TO_TICKS(10000)); continue; }
        xEventGroupWaitBits(s_network_event_group, MQTT_CONNECTED_BIT, pdFALSE, pdTRUE, portMAX_DELAY);

        struct stat st;
        if (stat(SPOOL_FILE_PATH, &st) == 0 && st.st_size > 0) {
            ESP_LOGW(TAG, "Cellular link restored. Initiating FIFO Backlog Replay...");
            FILE *f = fopen(SPOOL_FILE_PATH, "r");
            if (f != NULL) {
                char line[1024];
                while (fgets(line, sizeof(line), f) != NULL) {
                    line[strcspn(line, "\n")] = 0;
                    cJSON *saved_json = cJSON_Parse(line);
                    if (saved_json != NULL) {
                        cJSON_AddNumberToObject(saved_json, "r", 1);
                        char *replay_payload = cJSON_PrintUnformatted(saved_json);

                        if (is_mqtt_connected && mqtt_client != NULL) {
                            esp_mqtt_client_publish(mqtt_client, topic_tlm, replay_payload, 0, 1, 0);
                            ESP_LOGI(TAG, "Replayed -> %s", replay_payload);
                            vTaskDelay(pdMS_TO_TICKS(150));
                        } else { free(replay_payload); cJSON_Delete(saved_json); break; }
                        free(replay_payload); cJSON_Delete(saved_json);
                    }
                }
                fclose(f);
                if (is_mqtt_connected) {
                    remove(SPOOL_FILE_PATH);
                    ESP_LOGI(TAG, "Backlog replay complete. LittleFS spool cleared.");
                }
            }
        }
        vTaskDelay(pdMS_TO_TICKS(5000));
    }
}

// ==========================================
// WI-FI STATION SETUP (FOR TESTING)
// ==========================================
static void wifi_event_handler(void* arg, esp_event_base_t event_base, int32_t event_id, void* event_data) {
    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) esp_wifi_connect();
    else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
        ESP_LOGW(TAG, "Wi-Fi disconnected. Retrying...");
        xEventGroupClearBits(s_network_event_group, MQTT_CONNECTED_BIT); esp_wifi_connect();
    } else if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t* event = (ip_event_got_ip_t*) event_data;
        ESP_LOGI(TAG, "Wi-Fi got IP: " IPSTR, IP2STR(&event->ip_info.ip));
        if (current_route_state == ROUTE_LOCAL && is_internet_available()) {
            ESP_LOGI(TAG, "Fast recovery: Internet verified on new IP lease! Switching to Cloud Phase 1...");
            current_route_state = ROUTE_CLOUD; cloud_disconnect_count = 0; configure_mqtt_client();
        } else if (mqtt_client == NULL) {
            current_route_state = ROUTE_CLOUD; configure_mqtt_client();
        }
    }
}

static void wifi_init_sta(void) {
    esp_netif_create_default_wifi_sta();
    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    esp_wifi_init(&cfg);
    esp_event_handler_instance_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL, NULL);
    esp_event_handler_instance_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL, NULL);

    wifi_config_t wifi_config = {0};
    strcpy((char *)wifi_config.sta.ssid, wifi_ssid);
    strcpy((char *)wifi_config.sta.password, wifi_pass);
    esp_wifi_set_mode(WIFI_MODE_STA);
    esp_wifi_set_config(WIFI_IF_STA, &wifi_config);
    esp_wifi_start();
}

// ==========================================
// APPLICATION MAIN
// ==========================================
void app_main(void) {
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);
    
    init_littlefs();
    init_dynamic_identity();

    // 1. Initialize TCP/IP and events
    esp_netif_init();
    esp_event_loop_create_default();

    // 2. Configure the Physical Setup Switch (GPIO 32, with Pull-Up Enabled)
    gpio_config_t switch_cfg = {
        .pin_bit_mask = (1ULL << PROVISION_SWITCH_GPIO),
        .mode = GPIO_MODE_INPUT,
        .pull_up_en = GPIO_PULLUP_ENABLE,
        .pull_down_en = GPIO_PULLDOWN_DISABLE,
        .intr_type = GPIO_INTR_DISABLE
    };
    gpio_config(&switch_cfg);
    vTaskDelay(pdMS_TO_TICKS(10));

    // 3. CAPTIVE PORTAL PROVISIONING MODE
    if (ENABLE_HARDCODED_TESTING == 0 && gpio_get_level(PROVISION_SWITCH_GPIO) == 0) {
        ESP_LOGW(TAG, "===============================================");
        ESP_LOGW(TAG, " CONFIG MODE TRIGGERED (SLIDE SWITCH GND)      ");
        ESP_LOGW(TAG, " Broadcasting SoftAP Captive Portal...         ");
        ESP_LOGW(TAG, "===============================================");
        
        wifi_init_softap();
        xTaskCreate(dns_server_task, "dns_task", 3072, NULL, 5, NULL);
        start_captive_web_server();

        while(1) vTaskDelay(pdMS_TO_TICKS(1000)); 
    }

    // 4. NORMAL RUN MODE (Switch is HIGH)
    ESP_LOGI(TAG, "RUN MODE DETECTED. Loading NVS Configurations...");
    
    nvs_handle_t h;
    if (nvs_open("senseable", NVS_READONLY, &h) == ESP_OK) {
        size_t len;
        len = sizeof(wifi_ssid); nvs_get_str(h, "wifi_ssid", wifi_ssid, &len);
        len = sizeof(wifi_pass); nvs_get_str(h, "wifi_pass", wifi_pass, &len);
        len = sizeof(cellular_apn); nvs_get_str(h, "cell_apn", cellular_apn, &len);
        len = sizeof(sec_broker_uri); nvs_get_str(h, "sec_uri", sec_broker_uri, &len);
        len = sizeof(tenant_id); nvs_get_str(h, "tenant_id", tenant_id, &len);
        uint8_t mode = 0;
        if (nvs_get_u8(h, "hw_mode", &mode) == ESP_OK) current_hw_mode = (HardwareConfig_t)mode;
        nvs_close(h);
    }
    
    build_mqtt_topics();

    i2c_mutex = xSemaphoreCreateMutex();
    data_mutex = xSemaphoreCreateMutex();
    task_tracking_mutex = xSemaphoreCreateMutex();
    s_network_event_group = xEventGroupCreate();
    s_hardware_event_group = xEventGroupCreate();

    init_actuators();

    if (i2c_master_init() == ESP_OK) {
        i2c_device_config_t ds_cfg = { .dev_addr_length = I2C_ADDR_BIT_LEN_7, .device_address = DS3231_ADDR, .scl_speed_hz = I2C_MASTER_FREQ_HZ };
        i2c_master_bus_add_device(bus_handle, &ds_cfg, &ds3231_handle);
        ESP_LOGI(TAG, "DS3231 RTC successfully mapped to master bus.");

        struct timeval tv = { .tv_sec = get_rtc_epoch(), .tv_usec = 0 };
        settimeofday(&tv, NULL);
        ESP_LOGI(TAG, "ESP32 OS Time successfully synced with DS3231 hardware.");
    } else {
        ESP_LOGE(TAG, "Failed to initialize I2C master peripheral engine.");
    }

    // DELAY ADDED HERE: Allow massive A7670C power surge and LLC to stabilize before scanning
    vTaskDelay(pdMS_TO_TICKS(2000));
    scan_i2c_bus();

    xTaskCreate(ads_reader_task, "adc_worker", 3072, NULL, 5, NULL);
    xTaskCreate(telemetry_builder_task, "tlm_json", 4096, NULL, 5, NULL);
    xTaskCreate(discovery_builder_task, "disco_json", 4096, NULL, 4, NULL);
    xTaskCreate(backlog_replay_task, "fifo_replay", 4096, NULL, 4, NULL);

    if (current_hw_mode == HW_MODE_CELLULAR) {
        ESP_LOGI(TAG, ">>> BOOTING CELLULAR MODEM (A7670C) <<<");
        xTaskCreate(cellular_task, "cellular", 6144, NULL, 5, NULL);
    } else {
        ESP_LOGI(TAG, ">>> BOOTING WI-FI STATION <<<");
        wifi_init_sta(); 
        xTaskCreate(cloud_watchdog_task, "cloud_wdog", 2048, NULL, 3, NULL); 
    }
}