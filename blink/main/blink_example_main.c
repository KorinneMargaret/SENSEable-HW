/**
 * Project: Environment-Agnostic IoT Monitoring Framework
 * Author: Korinne Margaret V. Sasil, Mikhail Alexi D. Hatulan
 * Institute: University of San Carlos, Talamban Campus
 */

#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "freertos/event_groups.h"
#include "esp_system.h"
#include "esp_event.h"
#include "esp_netif.h"
#include "esp_netif_ppp.h"
#include "esp_modem_api.h"
#include "driver/uart.h"
#include "nvs_flash.h"
#include "nvs.h" 
#include "mqtt_client.h"
#include "driver/i2c_master.h"
#include "driver/gpio.h"
#include "driver/ledc.h"
#include "esp_log.h"
#include "cJSON.h"
#include "esp_mac.h" 

#include "credentials.h"

static const char *TAG = "THESIS_NODE";

#define TELEMETRY_INTERVAL_MS   10000
#define DISCOVERY_HEARTBEAT_MS  60000

// ==========================================
// CELLULAR / MODEM CONFIGURATION
// ==========================================
#define MODEM_UART_NUM          UART_NUM_1
#define MODEM_TX_PIN            17
#define MODEM_RX_PIN            16
#define MODEM_RTS_PIN           -1
#define MODEM_CTS_PIN           -1
#define MODEM_BAUDRATE          115200

// ==========================================
// RTC DS3231 / DS1307 DEFINITIONS
// ==========================================
#define RTC_I2C_ADDR            0x68
#define RTC_REG_SECONDS         0x00

static i2c_master_dev_handle_t rtc_handle = NULL;

// ==========================================
// DYNAMIC TOPIC & ID BUFFERS
// ==========================================
char node_id[32];
char tenant_id[32];
char topic_tlm[128];
char topic_cmd[128];
char topic_ack[128];
char topic_disco[128];
char topic_status[128];

#define FLOATING_LEAK_MIN   4500
#define FLOATING_LEAK_MAX   5000

// ==========================================
// ACTUATOR PIN MAPS & MUX LAYOUT
// ==========================================
#define NUM_ACTUATORS       6  

const int actuator_gpios[NUM_ACTUATORS] = {4, 25, 13, 14, 26, 27};

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
// HARDWARE CONSTANTS & GLOBAL STATE
// ==========================================
#define I2C_MASTER_SDA_IO           21
#define I2C_MASTER_SCL_IO           22
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
SemaphoreHandle_t task_tracking_mutex;

bool port_active[4][4] = {
    {true, true, true, true},
    {true, true, true, true},
    {true, true, true, true},
    {true, true, true, true}
};

volatile bool is_mqtt_connected = false;

static EventGroupHandle_t s_ppp_event_group;
#define PPP_CONNECTED_BIT           BIT0

static EventGroupHandle_t s_hardware_event_group;
#define I2C_RESCAN_REQUIRED_BIT     BIT0

typedef struct {
    uint8_t address;
    int16_t port_values[4];
    bool is_online;
} NodeData;

NodeData global_node_data[4];

typedef struct {
    int target_idx;
    int duration_ms;
    char cid[64];
} auto_shutoff_args_t;

TaskHandle_t auto_shutoff_task_handles[NUM_ACTUATORS] = {NULL, NULL, NULL, NULL, NULL, NULL};
auto_shutoff_args_t global_timer_args[NUM_ACTUATORS]; 

static esp_err_t i2c_master_init(void);

// ==========================================
// BCD-TO-DECIMAL HELPERS FOR RTC
// ==========================================
static uint8_t bcd2dec(uint8_t val) {
    return ((val >> 4) * 10) + (val & 0x0F);
}

static uint64_t get_rtc_timestamp(void) {
    if (rtc_handle == NULL) {
        return (uint64_t)time(NULL);
    }

    uint8_t reg = RTC_REG_SECONDS;
    uint8_t data[7] = {0};

    if (xSemaphoreTake(i2c_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
        esp_err_t ret = i2c_master_transmit_receive(rtc_handle, &reg, 1, data, sizeof(data), 100);
        xSemaphoreGive(i2c_mutex);

        if (ret == ESP_OK) {
            struct tm tm;
            tm.tm_sec  = bcd2dec(data[0] & 0x7F);
            tm.tm_min  = bcd2dec(data[1] & 0x7F);
            tm.tm_hour = bcd2dec(data[2] & 0x3F);
            tm.tm_mday = bcd2dec(data[4] & 0x3F);
            tm.tm_mon  = bcd2dec(data[5] & 0x1F) - 1;
            tm.tm_year = bcd2dec(data[6]) + 100; // 2000s Epoch Offset
            tm.tm_isdst = 0;

            time_t t = mktime(&tm);
            if (t != -1) {
                return (uint64_t)t;
            }
        }
    }
    return (uint64_t)time(NULL); 
}

// ==========================================
// DYNAMIC NODE ID GENERATION
// ==========================================
static void init_dynamic_identity(void) {
    uint8_t mac[6];
    esp_read_mac(mac, ESP_MAC_EFUSE_FACTORY);
    
    sprintf(node_id, "NODE-%02X%02X%02X", mac[3], mac[4], mac[5]);
    
    strcpy(tenant_id, DEFAULT_TENANT_ID);
    nvs_handle_t h;
    if (nvs_open("senseable", NVS_READONLY, &h) == ESP_OK) {
        size_t len = sizeof(tenant_id);
        nvs_get_str(h, "tenant_id", tenant_id, &len);
        nvs_close(h);
    }
    
    sprintf(topic_tlm,   "%s/%s/%s/tlm",   MQTT_TOPIC_ROOT, tenant_id, node_id);
    sprintf(topic_cmd,   "%s/%s/%s/cmd",   MQTT_TOPIC_ROOT, tenant_id, node_id);
    sprintf(topic_ack,   "%s/%s/%s/ack",   MQTT_TOPIC_ROOT, tenant_id, node_id);
    sprintf(topic_disco, "%s/%s/%s/disco", MQTT_TOPIC_ROOT, tenant_id, node_id);
    sprintf(topic_status,"%s/%s/%s/status",MQTT_TOPIC_ROOT, tenant_id, node_id);
    
    ESP_LOGI(TAG, "====================================");
    ESP_LOGI(TAG, "PROVISIONED AS CELLULAR NODE: %s / %s", tenant_id, node_id);
    ESP_LOGI(TAG, "Command Topic: %s", topic_cmd);
    ESP_LOGI(TAG, "====================================");
}

// ==========================================
// TWO-STEP ACKNOWLEDGEMENT LOGIC
// ==========================================
static void send_command_ack(const char *cid, const char *status, const char *details) {
    if (!is_mqtt_connected || mqtt_client == NULL) {
        return;
    }

    cJSON *ack_root = cJSON_CreateObject();
    if (ack_root == NULL) return;

    cJSON_AddStringToObject(ack_root, "t", "ack");
    cJSON_AddNumberToObject(ack_root, "v", 1);
    cJSON_AddStringToObject(ack_root, "tid", tenant_id);
    cJSON_AddStringToObject(ack_root, "nid", node_id);
    cJSON_AddStringToObject(ack_root, "cid", cid ? cid : "unknown");
    cJSON_AddStringToObject(ack_root, "status", status);
    cJSON_AddStringToObject(ack_root, "details", details ? details : "");
    cJSON_AddNumberToObject(ack_root, "ts", (double)get_rtc_timestamp());

    char *payload = cJSON_PrintUnformatted(ack_root);
    if (payload != NULL) {
        esp_mqtt_client_publish(mqtt_client, topic_ack, payload, 0, 1, 0);
        ESP_LOGI(TAG, "Command ACK published -> Status: %s | ID: %s", status, cid ? cid : "unknown");
        free(payload);
    }
    cJSON_Delete(ack_root);
}

// ==========================================
// BIT-BANG I2C BUS RECOVERY ROUTINE
// ==========================================
static void recover_i2c_bus(void) {
    ESP_LOGW(TAG, "Executing software bit-bang recovery routine...");

    if (xSemaphoreTake(i2c_mutex, portMAX_DELAY) == pdTRUE) {
        for (int i = 0; i < 4; i++) {
            if (ads_handles[i] != NULL) {
                i2c_master_bus_rm_device(ads_handles[i]);
                ads_handles[i] = NULL;
            }
        }
        if (rtc_handle != NULL) {
            i2c_master_bus_rm_device(rtc_handle);
            rtc_handle = NULL;
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
            ESP_LOGE(TAG, "Fatal fault re-instantiating master bus.");
        }

        xSemaphoreGive(i2c_mutex);
    }
}

// ==========================================
// ACTUATORS DRIVER SETUP
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
void auto_shutoff_task(void *pvParameter) {
    auto_shutoff_args_t *args = (auto_shutoff_args_t *)pvParameter;
    vTaskDelay(pdMS_TO_TICKS(args->duration_ms));
    
    int mapped_gpio = actuator_gpios[args->target_idx];
    ledc_channel_t mapped_chan = actuator_channels[args->target_idx];
    
    ledc_stop(ACTUATOR_LEDC_MODE, mapped_chan, 0);
    gpio_set_level(mapped_gpio, 0);
    
    ESP_LOGW(TAG, ">>> Auto-shutoff triggered for OUT%d <<<", args->target_idx + 1);
    send_command_ack(args->cid, "completed", "Auto-shutoff execution window expired safely");

    if (xSemaphoreTake(task_tracking_mutex, portMAX_DELAY) == pdTRUE) {
        auto_shutoff_task_handles[args->target_idx] = NULL;
        xSemaphoreGive(task_tracking_mutex);
    }

    vTaskDelete(NULL);
}

// ==========================================
// CELLULAR PPP EVENT HANDLERS & INITIALIZATION
// ==========================================
static void ppp_event_handler(void *arg, esp_event_base_t event_base, int32_t event_id, void *event_data) {
    if (event_base == NETIF_PPP_STATUS && event_id == NETIF_PPP_ERRORCONNECT) {
        ESP_LOGE(TAG, "Cellular PPP Connection Failed");
        xEventGroupClearBits(s_ppp_event_group, PPP_CONNECTED_BIT);
    } else if (event_base == IP_EVENT && event_id == IP_EVENT_PPP_GOT_IP) {
        ip_event_got_ip_t *event = (ip_event_got_ip_t *)event_data;
        ESP_LOGI(TAG, "Cellular Link Active. Got IP: " IPSTR, IP2STR(&event->ip_info.ip));
        xEventGroupSetBits(s_ppp_event_group, PPP_CONNECTED_BIT);
    } else if (event_base == IP_EVENT && event_id == IP_EVENT_PPP_LOST_IP) {
        ESP_LOGW(TAG, "Cellular Link Lost IP.");
        xEventGroupClearBits(s_ppp_event_group, PPP_CONNECTED_BIT);
    }
}

static void mqtt_event_handler(void *handler_args, esp_event_base_t base, int32_t event_id, void *event_data) {
    esp_mqtt_event_handle_t event = event_data;
    esp_mqtt_client_handle_t client = event->client;

    if (event_id == MQTT_EVENT_CONNECTED) {
        ESP_LOGI(TAG, "SUCCESS! Secure Cellular Connection to Broker Established!");
        is_mqtt_connected = true;
        xEventGroupSetBits(s_hardware_event_group, I2C_RESCAN_REQUIRED_BIT);
        
        esp_mqtt_client_subscribe(client, topic_cmd, 1);
        esp_mqtt_client_publish(client, topic_status, "{\"t\":\"lwt\",\"status\":\"online\"}", 0, 1, 1);
    } 
    else if (event_id == MQTT_EVENT_DISCONNECTED) {
        ESP_LOGW(TAG, "MQTT Broker Disconnected.");
        is_mqtt_connected = false;
    } 
    else if (event_id == MQTT_EVENT_DATA) {
        if (event->data_len >= 1024) return;

        char json_string[1024];
        memcpy(json_string, event->data, event->data_len);
        json_string[event->data_len] = '\0';
            
        cJSON *root = cJSON_Parse(json_string);
        if (root) {
            cJSON *cid_item = cJSON_GetObjectItem(root, "cid");
            const char *cid_str = (cid_item && cJSON_IsString(cid_item)) ? cid_item->valuestring : NULL;

            if (!cid_str) {
                cJSON_Delete(root);
                return;
            }

            cJSON *act_item = cJSON_GetObjectItem(root, "action");
            if (act_item && cJSON_IsString(act_item)) {
                const char *action = act_item->valuestring;
                
                if (strcmp(action, "bus_recovery") == 0) {
                    send_command_ack(cid_str, "started", "Beginning I2C recovery cycle");
                    recover_i2c_bus();
                    xEventGroupSetBits(s_hardware_event_group, I2C_RESCAN_REQUIRED_BIT);
                    send_command_ack(cid_str, "completed", "I2C bus recovery complete");
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
                            bool actuator_active_state = false;

                            if (strcmp(mode_str, "bin") == 0) {
                                cJSON *state_item = cJSON_GetObjectItem(root, "state");
                                if (state_item && cJSON_IsNumber(state_item)) {
                                    int state_val = state_item->valueint;
                                    actuator_active_state = (state_val > 0);
                                    ledc_stop(ACTUATOR_LEDC_MODE, mapped_chan, state_val);
                                    gpio_set_level(mapped_gpio, state_val);
                                }
                            } 
                            else if (strcmp(mode_str, "pwm") == 0) {
                                cJSON *state_item = cJSON_GetObjectItem(root, "state");
                                cJSON *duty_item  = cJSON_GetObjectItem(root, "duty");

                                if (state_item && cJSON_IsNumber(state_item) && state_item->valueint == 0) {
                                    ledc_set_duty(ACTUATOR_LEDC_MODE, mapped_chan, 0);
                                    ledc_update_duty(ACTUATOR_LEDC_MODE, mapped_chan);
                                    actuator_active_state = false;
                                }
                                else if (duty_item && cJSON_IsNumber(duty_item)) {
                                    int duty_val = duty_item->valueint;
                                    actuator_active_state = (duty_val > 0);
                                    ledc_set_duty(ACTUATOR_LEDC_MODE, mapped_chan, duty_val);
                                    ledc_update_duty(ACTUATOR_LEDC_MODE, mapped_chan);
                                }
                            }
                            
                            if (xSemaphoreTake(task_tracking_mutex, portMAX_DELAY) == pdTRUE) {
                                if (auto_shutoff_task_handles[target_idx] != NULL) {
                                    vTaskDelete(auto_shutoff_task_handles[target_idx]);
                                    auto_shutoff_task_handles[target_idx] = NULL;
                                }
                                xSemaphoreGive(task_tracking_mutex);
                            }

                            if (actuator_active_state) {
                                if (duration_ms > 0) {
                                    send_command_ack(cid_str, "started", "Actuator driven high, auto-shutoff armed");
                                    global_timer_args[target_idx].target_idx = target_idx;
                                    global_timer_args[target_idx].duration_ms = duration_ms;
                                    strncpy(global_timer_args[target_idx].cid, cid_str, sizeof(global_timer_args[target_idx].cid) - 1);
                                    
                                    if (xSemaphoreTake(task_tracking_mutex, portMAX_DELAY) == pdTRUE) {
                                        xTaskCreate(auto_shutoff_task, "auto_shutoff", 2048, (void *)&global_timer_args[target_idx], 5, &auto_shutoff_task_handles[target_idx]);
                                        xSemaphoreGive(task_tracking_mutex);
                                    }
                                } else {
                                    send_command_ack(cid_str, "started", "Actuator driven high indefinitely");
                                }
                            } else {
                                send_command_ack(cid_str, "stopped", "Actuator set to default idle state");
                            }
                        } else {
                            send_command_ack(cid_str, "failed", "Port limit out of bounds");
                        }
                    } else {
                        send_command_ack(cid_str, "failed", "Missing dynamic execution parameters");
                    }
                }
            }
            cJSON_Delete(root);
        }
    }
}

static void cellular_network_init(void) {
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());

    s_ppp_event_group = xEventGroupCreate();

    esp_modem_dte_config_t dte_config = ESP_MODEM_DTE_DEFAULT_CONFIG();
    dte_config.uart_config.tx_io_num = MODEM_TX_PIN;
    dte_config.uart_config.rx_io_num = MODEM_RX_PIN;
    dte_config.uart_config.rts_io_num = MODEM_RTS_PIN;
    dte_config.uart_config.cts_io_num = MODEM_CTS_PIN;
    dte_config.uart_config.baud_rate = MODEM_BAUDRATE;
    dte_config.uart_config.port_num = MODEM_UART_NUM;

    esp_netif_config_t ppp_netif_config = ESP_NETIF_CONFIG_DEFAULT_PPP();
    esp_netif_t *esp_netif = esp_netif_new(&ppp_netif_config);
    assert(esp_netif);

    esp_modem_dce_config_t dce_config = ESP_MODEM_DCE_DEFAULT_CONFIG("internet"); // APN Name

    esp_modem_dce_t *dce = esp_modem_new_dev(ESP_MODEM_DCE_SIM800, &dte_config, &dce_config, esp_netif);
    assert(dce);

    ESP_ERROR_CHECK(esp_event_handler_instance_register(NETIF_PPP_STATUS, ESP_EVENT_ANY_ID, &ppp_event_handler, NULL, NULL));
    ESP_ERROR_CHECK(esp_event_handler_instance_register(IP_EVENT, IP_EVENT_PPP_GOT_IP, &ppp_event_handler, NULL, NULL));

    ESP_LOGI(TAG, "Dialing PPP Cellular Link...");
    ESP_ERROR_CHECK(esp_modem_set_mode(dce, ESP_MODEM_MODE_DATA));

    xEventGroupWaitBits(s_ppp_event_group, PPP_CONNECTED_BIT, pdFALSE, pdTRUE, portMAX_DELAY);

    esp_mqtt_client_config_t mqtt_cfg = {
        .broker.address.uri = MQTT_BROKER_URI,
        .credentials.username = MQTT_USERNAME,
        .credentials.authentication.password = MQTT_PASSWORD,
        .broker.verification.certificate = mosqmq_root_ca,
        .session.last_will.topic  = topic_status,
        .session.last_will.msg    = "{\"t\":\"lwt\",\"status\":\"offline\"}",
        .session.last_will.qos    = 1,
        .session.last_will.retain = 1,
        .session.keepalive = 15,
    };
    mqtt_client = esp_mqtt_client_init(&mqtt_cfg);
    esp_mqtt_client_register_event(mqtt_client, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
    esp_mqtt_client_start(mqtt_client);
}

// ==========================================
// I2C SCANNING & RTC DEVICE ATTACHMENT
// ==========================================
static void scan_i2c_bus(void) {
    uint8_t found_this_run = 0;

    if (xSemaphoreTake(i2c_mutex, pdMS_TO_TICKS(200)) == pdTRUE) {
        if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
            
            // Check RTC Presence First
            if (i2c_master_probe(bus_handle, RTC_I2C_ADDR, 100) == ESP_OK) {
                if (rtc_handle == NULL) {
                    i2c_device_config_t dev_cfg = {
                        .dev_addr_length = I2C_ADDR_BIT_LEN_7,
                        .device_address = RTC_I2C_ADDR,
                        .scl_speed_hz = I2C_MASTER_FREQ_HZ,
                    };
                    i2c_master_bus_add_device(bus_handle, &dev_cfg, &rtc_handle);
                    ESP_LOGI(TAG, "Real-Time Clock (RTC DS3231/DS1307) Verified at 0x68.");
                }
            }

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
// WORKING TASKS
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
                        tx_err = i2c_master_transmit(ads_handles[i], config_data, sizeof(config_data), 100);
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
                            rx_err = i2c_master_transmit_receive(ads_handles[i], &reg_pointer, 1, read_buf, sizeof(read_buf), 100);
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
            ESP_LOGW(TAG, "Hardware link drop caught! Triggering bit-bang recovery...");
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
        cJSON_AddStringToObject(root, "tid", tenant_id);
        cJSON_AddStringToObject(root, "nid", node_id);
        cJSON_AddNumberToObject(root, "ts", (double)get_rtc_timestamp());

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
            esp_mqtt_client_publish(mqtt_client, topic_tlm, payload_string, 0, 1, 0);
            ESP_LOGI(TAG, "Telemetry Dispatched via Cellular: %s", payload_string);
        }
        free(payload_string);
        cJSON_Delete(root);

        vTaskDelay(pdMS_TO_TICKS(TELEMETRY_INTERVAL_MS));
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
    static TickType_t last_disco_publish = 0;

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
            pdMS_TO_TICKS(DISCOVERY_HEARTBEAT_MS)
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

        bool topology_changed = (current_detailed_topology != last_detailed_topology);
        bool heartbeat_due = (xTaskGetTickCount() - last_disco_publish) > pdMS_TO_TICKS(DISCOVERY_HEARTBEAT_MS);

        if (!topology_changed && !heartbeat_due) {
            continue; 
        }
        
        last_detailed_topology = current_detailed_topology;
        last_disco_publish = xTaskGetTickCount();

        cJSON *root = cJSON_CreateObject();
        cJSON_AddStringToObject(root, "t", "disco");
        cJSON_AddNumberToObject(root, "v", 1);
        cJSON_AddStringToObject(root, "tid", tenant_id);
        cJSON_AddStringToObject(root, "nid", node_id);
        cJSON_AddNumberToObject(root, "ts", (double)get_rtc_timestamp());
        cJSON_AddNumberToObject(root, "tlm_interval_ms", TELEMETRY_INTERVAL_MS);

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
            esp_mqtt_client_publish(mqtt_client, topic_disco, payload_string, 0, 1, 1);
            ESP_LOGW(TAG, "New Discovery Packet Dispatched: %s", payload_string);
        }
        free(payload_string);
        cJSON_Delete(root);
    }
}

// ==========================================
// APP MAIN ENTRY
// ==========================================
void app_main(void) {
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    init_dynamic_identity();

    i2c_mutex = xSemaphoreCreateMutex();
    data_mutex = xSemaphoreCreateMutex();
    task_tracking_mutex = xSemaphoreCreateMutex();
    s_hardware_event_group = xEventGroupCreate();

    init_actuators(); 

    if (i2c_master_init() != ESP_OK) {
        ESP_LOGE(TAG, "Failed to initialize I2C master engine.");
        vTaskSuspend(NULL);
    }

    scan_i2c_bus();

    xTaskCreate(ads_reader_task, "unified_adc_worker", 3072, NULL, 5, NULL);
    xTaskCreate(telemetry_builder_task, "tlm_json_task", 4096, NULL, 5, NULL);
    xTaskCreate(discovery_builder_task, "disco_json_task", 4096, NULL, 5, NULL);

    cellular_network_init();
}