/**
 * Project: Environment-Agnostic IoT Monitoring Framework
 * Architecture: Adaptive IoT Firmware with Edge Failover
 * Author: Korinne Margaret V. Sasil, Mikhail Alexi D. Hatulan
 * Institute: University of San Carlos, Talamban Campus
 */

#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>
#include <sys/socket.h>
#include <netdb.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "freertos/event_groups.h"

#include "esp_system.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_netif.h"
#include "nvs_flash.h"
#include "nvs.h" 
#include "mqtt_client.h"
#include "esp_http_server.h"
#include "driver/i2c_master.h"
#include "driver/gpio.h"
#include "driver/ledc.h"
#include "esp_log.h"
#include "esp_netif_sntp.h"
#include "cJSON.h"
#include "esp_mac.h" 
#include "lwip/sockets.h"
#include "lwip/dns.h"

#include "credentials.h"

static const char *TAG = "THESIS_NODE";

#define TELEMETRY_INTERVAL_MS   10000
#define DISCOVERY_HEARTBEAT_MS  60000

// Slide Switch GPIO Assignment (GND = Config Mode, HIGH/PULLUP = Normal Operation)
#define PROVISION_SWITCH_GPIO   0       

// Maximum cloud reconnect failures before shifting route
#define MAX_CLOUD_FAILURES      3       

// Exponential backoff parameters for Cloud Ping Task
#define MIN_PING_INTERVAL_SEC   300     
#define MAX_PING_INTERVAL_SEC   3600    

// ==========================================
// SYSTEM ENUMS & STATE STORAGE
// ==========================================
typedef enum {
    HW_MODE_WIFI = 0,
    HW_MODE_CELLULAR = 1
} HardwareConfig_t;

typedef enum {
    ROUTE_CLOUD_FIRST = 0,
    ROUTE_LOCAL_FAILOVER = 1
} RoutingState_t;

static HardwareConfig_t current_hw_mode = HW_MODE_WIFI;
static RoutingState_t current_route_state = ROUTE_CLOUD_FIRST;
static int cloud_disconnect_count = 0;
static uint32_t current_ping_backoff_sec = MIN_PING_INTERVAL_SEC;

// NVS Provisioned Configuration Buffers
char wifi_ssid[64];
char wifi_pass[64];
char pri_broker_uri[128];
char pri_username[64];
char pri_password[64];
char sec_broker_uri[128];
char sec_username[64];
char sec_password[64];

// Dynamic Identifiers & Topics
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
// ACTUATOR MAPPINGS
// ==========================================
#define NUM_ACTUATORS       6  
const int actuator_gpios[NUM_ACTUATORS] = {4, 25, 13, 14, 16, 17};
const ledc_channel_t actuator_channels[NUM_ACTUATORS] = {
    LEDC_CHANNEL_0, LEDC_CHANNEL_1, LEDC_CHANNEL_2,
    LEDC_CHANNEL_3, LEDC_CHANNEL_4, LEDC_CHANNEL_5
};

#define ACTUATOR_LEDC_MODE          LEDC_LOW_SPEED_MODE
#define ACTUATOR_LEDC_TIMER         LEDC_TIMER_0
#define ACTUATOR_LEDC_RES           LEDC_TIMER_8_BIT   
#define ACTUATOR_LEDC_FREQ          5000               

// ==========================================
// I2C PERIPHERAL & GLOBAL OBJECTS
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

typedef struct {
    int target_idx;
    int duration_ms;
    char cid[64];
} auto_shutoff_args_t;

TaskHandle_t auto_shutoff_task_handles[NUM_ACTUATORS] = {NULL, NULL, NULL, NULL, NULL, NULL};
auto_shutoff_args_t global_timer_args[NUM_ACTUATORS]; 

// Forward declarations
static esp_err_t i2c_master_init(void);
static void configure_mqtt_client(void);

// ==========================================
// CAPTIVE PORTAL HTML INTERFACE
// ==========================================
static const char captive_portal_html[] = 
"<!DOCTYPE html><html><head><meta name='viewport' content='width=device-width,initial-scale=1'>"
"<style>body{font-family:Arial,sans-serif;margin:20px;background:#f4f4f9;}"
".card{background:#fff;padding:20px;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,0.1);max-width:400px;margin:auto;}"
"h2{color:#333;}label{font-weight:bold;display:block;margin-top:10px;}"
"input,select{width:100%;padding:8px;margin-top:4px;box-sizing:border-box;border:1px solid #ccc;border-radius:4px;}"
"button{margin-top:15px;width:100%;background:#007bff;color:#fff;border:none;padding:10px;border-radius:4px;font-size:16px;cursor:cursor;}"
"button:hover{background:#0056b3;}</style></head><body>"
"<div class='card'><h2>Node Setup</h2>"
"<form action='/save' method='POST'>"
"<label>Wi-Fi SSID</label><input type='text' name='ssid' required>"
"<label>Wi-Fi Password</label><input type='password' name='pass'>"
"<label>Tenant ID</label><input type='text' name='tenant' value='tenant-123'>"
"<label>Hardware Connection</label>"
"<select name='hw_mode'><option value='0'>Wi-Fi Station</option><option value='1'>Cellular Modem</option></select>"
"<label>Cloud Broker URI (Non-TLS)</label><input type='text' name='pri_uri' value='mqtt://broker.hivemq.com:1883'>"
"<label>Cloud Username</label><input type='text' name='pri_user'>"
"<label>Cloud Password</label><input type='password' name='pri_pass'>"
"<label>Local Edge URI (TLS)</label><input type='text' name='sec_uri' value='mqtts://10.44.142.162:8883'>"
"<label>Local Edge Username</label><input type='text' name='sec_user'>"
"<label>Local Edge Password</label><input type='password' name='sec_pass'>"
"<button type='submit'>Save Configuration</button>"
"</form></div></body></html>";

// ==========================================
// CAPTIVE PORTAL DNS & HTTP SERVER
// ==========================================
static void dns_server_task(void *pvParameters) {
    uint8_t rx_buffer[128];
    struct sockaddr_in ra;
    socklen_t addr_len = sizeof(ra);

    int sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
    if (sock < 0) {
        vTaskDelete(NULL);
        return;
    }

    struct sockaddr_in sa = {
        .sin_family = AF_INET,
        .sin_port = htons(53),
        .sin_addr.s_addr = htonl(INADDR_ANY)
    };
    bind(sock, (struct sockaddr *)&sa, sizeof(sa));

    while (1) {
        int len = recvfrom(sock, rx_buffer, sizeof(rx_buffer), 0, (struct sockaddr *)&ra, &addr_len);
        if (len > 12) {
            rx_buffer[2] |= 0x80; 
            rx_buffer[3] |= 0x80;
            rx_buffer[7] = 1;     
            
            uint8_t reply[128];
            memcpy(reply, rx_buffer, len);
            int idx = len;
            reply[idx++] = 0xc0; reply[idx++] = 0x0c; 
            reply[idx++] = 0x00; reply[idx++] = 0x01; 
            reply[idx++] = 0x00; reply[idx++] = 0x01; 
            reply[idx++] = 0x00; reply[idx++] = 0x00; 
            reply[idx++] = 0x00; reply[idx++] = 0x3c;
            reply[idx++] = 0x00; reply[idx++] = 0x04; 
            reply[idx++] = 192;  reply[idx++] = 168;  
            reply[idx++] = 4;    reply[idx++] = 1;

            sendto(sock, reply, idx, 0, (struct sockaddr *)&ra, addr_len);
        }
        vTaskDelay(pdMS_TO_TICKS(50));
    }
}

static esp_err_t http_root_handler(httpd_req_t *req) {
    httpd_resp_set_type(req, "text/html");
    return httpd_resp_send(req, captive_portal_html, HTTPD_RESP_USE_STRLEN);
}

static esp_err_t http_save_handler(httpd_req_t *req) {
    char buf[1024];
    int ret = httpd_req_recv(req, buf, sizeof(buf) - 1);
    if (ret <= 0) return ESP_FAIL;
    buf[ret] = '\0';

    nvs_handle_t h;
    if (nvs_open("senseable", NVS_READWRITE, &h) == ESP_OK) {
        char val[128];
        
        #define EXTRACT_AND_SAVE(key, param) \
            if (httpd_query_key_value(buf, key, val, sizeof(val)) == ESP_OK) { \
                nvs_set_str(h, param, val); \
            }

        EXTRACT_AND_SAVE("ssid", "wifi_ssid");
        EXTRACT_AND_SAVE("pass", "wifi_pass");
        EXTRACT_AND_SAVE("tenant", "tenant_id");
        EXTRACT_AND_SAVE("pri_uri", "pri_uri");
        EXTRACT_AND_SAVE("pri_user", "pri_user");
        EXTRACT_AND_SAVE("pri_pass", "pri_pass");
        EXTRACT_AND_SAVE("sec_uri", "sec_uri");
        EXTRACT_AND_SAVE("sec_user", "sec_user");
        EXTRACT_AND_SAVE("sec_pass", "sec_pass");

        if (httpd_query_key_value(buf, "hw_mode", val, sizeof(val)) == ESP_OK) {
            uint8_t mode = atoi(val);
            nvs_set_u8(h, "hw_mode", mode);
        }

        nvs_commit(h);
        nvs_close(h);
    }

    httpd_resp_send(req, "<h2>Configuration Saved! Rebooting Node...</h2>", HTTPD_RESP_USE_STRLEN);
    vTaskDelay(pdMS_TO_TICKS(2000));
    esp_restart();
    return ESP_OK;
}

static void start_captive_portal(void) {
    ESP_LOGW(TAG, "Slide switch in CONFIG mode. Starting Provisioning Portal...");
    esp_netif_create_default_wifi_ap();
    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    esp_wifi_init(&cfg);

    wifi_config_t ap_config = {
        .ap = {
            .ssid = "NODE-PROVISION-AP",
            .ssid_len = strlen("NODE-PROVISION-AP"),
            .channel = 1,
            .authmode = WIFI_AUTH_OPEN,
            .max_connection = 4
        }
    };
    esp_wifi_set_mode(WIFI_MODE_AP);
    esp_wifi_set_config(WIFI_IF_AP, &ap_config);
    esp_wifi_start();

    xTaskCreate(dns_server_task, "dns_task", 3072, NULL, 5, NULL);

    httpd_handle_t server = NULL;
    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    config.max_uri_handlers = 8;

    if (httpd_start(&server, &config) == ESP_OK) {
        httpd_uri_t uri_get = { .uri = "/", .method = HTTP_GET, .handler = http_root_handler };
        httpd_register_uri_handler(server, &uri_get);
        httpd_uri_t uri_save = { .uri = "/save", .method = HTTP_POST, .handler = http_save_handler };
        httpd_register_uri_handler(server, &uri_save);
    }
}

// ==========================================
// DYNAMIC NVS INITIALIZATION & IDENTITY
// ==========================================
static void load_nvs_credentials(void) {
    nvs_handle_t h;
    
    // Load defaults first
    strcpy(wifi_ssid, DEFAULT_WIFI_SSID);
    strcpy(wifi_pass, DEFAULT_WIFI_PASS);
    strcpy(pri_broker_uri, DEFAULT_CLOUD_BROKER_URI);
    strcpy(pri_username, DEFAULT_CLOUD_USERNAME);
    strcpy(pri_password, DEFAULT_CLOUD_PASSWORD);
    strcpy(sec_broker_uri, DEFAULT_EDGE_BROKER_URI);
    strcpy(sec_username, DEFAULT_EDGE_USERNAME);
    strcpy(sec_password, DEFAULT_EDGE_PASSWORD);
    strcpy(tenant_id, DEFAULT_TENANT_ID);
    current_hw_mode = HW_MODE_WIFI;

    // Configure Slide Switch Pin with Internal Pull-up
    gpio_config_t switch_cfg = {
        .pin_bit_mask = (1ULL << PROVISION_SWITCH_GPIO),
        .mode = GPIO_MODE_INPUT,
        .pull_up_en = GPIO_PULLUP_ENABLE,
        .pull_down_en = GPIO_PULLDOWN_DISABLE,
        .intr_type = GPIO_INTR_DISABLE
    };
    gpio_config(&switch_cfg);

    // Give time for pin voltage levels to settle
    vTaskDelay(pdMS_TO_TICKS(10));

    // If switch is flipped to LOW (grounded), enter Provisioning Mode directly
    bool force_portal = (gpio_get_level(PROVISION_SWITCH_GPIO) == 0);

    if (nvs_open("senseable", NVS_READONLY, &h) == ESP_OK) {
        size_t len;
        
        len = sizeof(wifi_ssid); nvs_get_str(h, "wifi_ssid", wifi_ssid, &len);
        len = sizeof(wifi_pass); nvs_get_str(h, "wifi_pass", wifi_pass, &len);
        len = sizeof(pri_broker_uri); nvs_get_str(h, "pri_uri", pri_broker_uri, &len);
        len = sizeof(pri_username); nvs_get_str(h, "pri_user", pri_username, &len);
        len = sizeof(pri_password); nvs_get_str(h, "pri_pass", pri_password, &len);
        len = sizeof(sec_broker_uri); nvs_get_str(h, "sec_uri", sec_broker_uri, &len);
        len = sizeof(sec_username); nvs_get_str(h, "sec_user", sec_username, &len);
        len = sizeof(sec_password); nvs_get_str(h, "sec_pass", sec_password, &len);
        len = sizeof(tenant_id); nvs_get_str(h, "tenant_id", tenant_id, &len);
        
        uint8_t mode = 0;
        if (nvs_get_u8(h, "hw_mode", &mode) == ESP_OK) {
            current_hw_mode = (HardwareConfig_t)mode;
        }
        nvs_close(h);
    } else {
        // NVS partition uninitialized or missing parameters
        force_portal = true;
    }

    if (force_portal) {
        start_captive_portal();
        // Hold execution inside captive portal mode indefinitely
        while (1) vTaskDelay(pdMS_TO_TICKS(1000));
    }
}

static void init_dynamic_identity(void) {
    uint8_t mac[6];
    esp_read_mac(mac, ESP_MAC_WIFI_STA);
    
    sprintf(node_id, "NODE-%02X%02X%02X", mac[3], mac[4], mac[5]);
    
    sprintf(topic_tlm,    "%s/%s/%s/tlm",    MQTT_TOPIC_ROOT, tenant_id, node_id);
    sprintf(topic_cmd,    "%s/%s/%s/cmd",    MQTT_TOPIC_ROOT, tenant_id, node_id);
    sprintf(topic_ack,    "%s/%s/%s/ack",    MQTT_TOPIC_ROOT, tenant_id, node_id);
    sprintf(topic_disco,  "%s/%s/%s/disco",  MQTT_TOPIC_ROOT, tenant_id, node_id);
    sprintf(topic_status, "%s/%s/%s/status", MQTT_TOPIC_ROOT, tenant_id, node_id);
    
    ESP_LOGI(TAG, "====================================");
    ESP_LOGI(TAG, "PROVISIONED AS: %s / %s", tenant_id, node_id);
    ESP_LOGI(TAG, "Hardware Mode: %s", (current_hw_mode == HW_MODE_WIFI) ? "Wi-Fi" : "Cellular");
    ESP_LOGI(TAG, "Command Topic: %s", topic_cmd);
    ESP_LOGI(TAG, "====================================");
}

// ==========================================
// BACKGROUND CLOUD HEALTH CHECK TASK
// ==========================================
static bool test_cloud_socket_connection(void) {
    char host[64] = {0};
    int port = 1883; 

    const char *uri_ptr = pri_broker_uri;
    if (strncmp(uri_ptr, "mqtt://", 7) == 0) {
        uri_ptr += 7;
        port = 1883;
    } else if (strncmp(uri_ptr, "mqtts://", 8) == 0) {
        uri_ptr += 8;
        port = 8883;
    }

    char *colon_ptr = strchr(uri_ptr, ':');
    if (colon_ptr) {
        strncpy(host, uri_ptr, colon_ptr - uri_ptr);
        port = atoi(colon_ptr + 1);
    } else {
        strcpy(host, uri_ptr);
    }

    struct hostent *he = gethostbyname(host);
    if (he == NULL) return false;

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return false;

    struct sockaddr_in server_addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr = *((struct in_addr *)he->h_addr_list[0])
    };

    struct timeval tv = { .tv_sec = 5, .tv_usec = 0 };
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    int res = connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr));
    close(sock);

    return (res == 0);
}

static void cloud_ping_task(void *pvParameters) {
    while (1) {
        vTaskDelay(pdMS_TO_TICKS(current_ping_backoff_sec * 1000));

        if (current_route_state == ROUTE_LOCAL_FAILOVER) {
            ESP_LOGI(TAG, "[HEALTH CHECK] Testing Cloud Broker Socket Reachability...");
            
            if (test_cloud_socket_connection()) {
                ESP_LOGW(TAG, "[HEALTH CHECK] Cloud restored! Reverting to ROUTE_CLOUD_FIRST...");
                current_route_state = ROUTE_CLOUD_FIRST;
                cloud_disconnect_count = 0;
                current_ping_backoff_sec = MIN_PING_INTERVAL_SEC;
                
                configure_mqtt_client();
            } else {
                current_ping_backoff_sec = current_ping_backoff_sec * 2;
                if (current_ping_backoff_sec > MAX_PING_INTERVAL_SEC) {
                    current_ping_backoff_sec = MAX_PING_INTERVAL_SEC;
                }
                ESP_LOGW(TAG, "[HEALTH CHECK] Cloud unreachable. Backoff extended to %" PRIu32 "s", current_ping_backoff_sec);
            }
        }
    }
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
    cJSON_AddNumberToObject(ack_root, "ts", (double)time(NULL));

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
// HARDWARE DRIVERS FOR ACTUATORS
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
// SYSTEM NETWORK & MQTT EVENT HANDLERS
// ==========================================
static void wifi_event_handler(void* arg, esp_event_base_t event_base, int32_t event_id, void* event_data) {
    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
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
        ESP_LOGI(TAG, "MQTT Connection Established! Route: %s", 
                 (current_route_state == ROUTE_CLOUD_FIRST) ? "CLOUD_FIRST" : "LOCAL_FAILOVER");
        is_mqtt_connected = true;
        cloud_disconnect_count = 0;
        xEventGroupSetBits(s_hardware_event_group, I2C_RESCAN_REQUIRED_BIT);
        
        esp_mqtt_client_subscribe(client, topic_cmd, 1);
        esp_mqtt_client_publish(client, topic_status, "{\"t\":\"lwt\",\"status\":\"online\"}", 0, 1, 1);
    } 
    else if (event_id == MQTT_EVENT_DISCONNECTED) {
        ESP_LOGW(TAG, "MQTT Broker Disconnected.");
        is_mqtt_connected = false;

        if (current_route_state == ROUTE_CLOUD_FIRST) {
            cloud_disconnect_count++;
            ESP_LOGW(TAG, "Cloud disconnect count: %d/%d", cloud_disconnect_count, MAX_CLOUD_FAILURES);
            
            if (cloud_disconnect_count >= MAX_CLOUD_FAILURES) {
                ESP_LOGE(TAG, "Cloud limit reached! SHIFTING ROUTE TO ROUTE_LOCAL_FAILOVER...");
                current_route_state = ROUTE_LOCAL_FAILOVER;
                configure_mqtt_client();
            }
        }
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
            if (!cid_str) { cJSON_Delete(root); return; }

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
                                cJSON *duty_item  = cJSON_GetObjectItem(root, "duty");
                                if (duty_item && cJSON_IsNumber(duty_item)) {
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

                            if (actuator_active_state && duration_ms > 0) {
                                send_command_ack(cid_str, "started", "Actuator driven high, auto-shutoff armed");
                                global_timer_args[target_idx].target_idx = target_idx;
                                global_timer_args[target_idx].duration_ms = duration_ms;
                                strncpy(global_timer_args[target_idx].cid, cid_str, sizeof(global_timer_args[target_idx].cid) - 1);
                                
                                if (xSemaphoreTake(task_tracking_mutex, portMAX_DELAY) == pdTRUE) {
                                    xTaskCreate(auto_shutoff_task, "auto_shutoff", 2048, (void *)&global_timer_args[target_idx], 5, &auto_shutoff_task_handles[target_idx]);
                                    xSemaphoreGive(task_tracking_mutex);
                                }
                            } else {
                                send_command_ack(cid_str, "completed", "Actuator state applied");
                            }
                        }
                    }
                }
            }
            cJSON_Delete(root);
        }
    }
}

// Dynamic Client Re-configuration Machine
static void configure_mqtt_client(void) {
    if (mqtt_client != NULL) {
        esp_mqtt_client_stop(mqtt_client);
        esp_mqtt_client_destroy(mqtt_client);
        mqtt_client = NULL;
    }

    esp_mqtt_client_config_t mqtt_cfg = {
        .session.last_will.topic  = topic_status,
        .session.last_will.msg    = "{\"t\":\"lwt\",\"status\":\"offline\"}",
        .session.last_will.qos    = 1,
        .session.last_will.retain = 1,
        .session.keepalive        = 15,
    };

    if (current_route_state == ROUTE_CLOUD_FIRST) {
        ESP_LOGI(TAG, "Configuring MQTT Engine -> Primary Cloud Broker (%s)", pri_broker_uri);
        mqtt_cfg.broker.address.uri = pri_broker_uri;
        mqtt_cfg.credentials.username = pri_username;
        mqtt_cfg.credentials.authentication.password = pri_password;
        mqtt_cfg.broker.verification.certificate = NULL; 
    } else {
        ESP_LOGW(TAG, "Configuring MQTT Engine -> Local Edge Broker (%s)", sec_broker_uri);
        mqtt_cfg.broker.address.uri = sec_broker_uri;
        mqtt_cfg.credentials.username = sec_username;
        mqtt_cfg.credentials.authentication.password = sec_password;
        mqtt_cfg.broker.verification.certificate = mosqmq_root_ca; 
    }

    mqtt_client = esp_mqtt_client_init(&mqtt_cfg);
    esp_mqtt_client_register_event(mqtt_client, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
    esp_mqtt_client_start(mqtt_client);
}

static void obtain_time(void) {
    esp_sntp_config_t config = ESP_NETIF_SNTP_DEFAULT_CONFIG("pool.ntp.org");
    esp_netif_sntp_init(&config);
    if (esp_netif_sntp_sync_wait(pdMS_TO_TICKS(30000)) == ESP_OK) {
        ESP_LOGI(TAG, "System time synced over SNTP.");
    }
}

static void network_init(void) {
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());

    if (current_hw_mode == HW_MODE_WIFI) {
        ESP_LOGI(TAG, "Hardware mode: INITIALIZING WI-FI INTERFACE");
        esp_netif_create_default_wifi_sta();
        s_wifi_event_group = xEventGroupCreate();
        wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
        ESP_ERROR_CHECK(esp_wifi_init(&cfg));

        ESP_ERROR_CHECK(esp_event_handler_instance_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL, NULL));
        ESP_ERROR_CHECK(esp_event_handler_instance_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL, NULL));

        wifi_config_t wifi_config = { 0 };
        strncpy((char *)wifi_config.sta.ssid, wifi_ssid, sizeof(wifi_config.sta.ssid));
        strncpy((char *)wifi_config.sta.password, wifi_pass, sizeof(wifi_config.sta.password));

        ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
        ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
        ESP_ERROR_CHECK(esp_wifi_start());

        EventBits_t bits = xEventGroupWaitBits(s_wifi_event_group, WIFI_CONNECTED_BIT, pdFALSE, pdTRUE, pdMS_TO_TICKS(20000));
        if (bits & WIFI_CONNECTED_BIT) {
            obtain_time();
        }
    } else {
        ESP_LOGI(TAG, "Hardware mode: INITIALIZING CELLULAR MODEM DRIVER");
        // Place cellular PPP network interface setup logic here
    }

    configure_mqtt_client();
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
// FREERTOS SAMPLING & WORKER TASKS
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
            ESP_LOGW(TAG, "Hardware drop detected! Triggering bit-bang bus recovery...");
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
            esp_mqtt_client_publish(mqtt_client, topic_tlm, payload_string, 0, 1, 0);
            ESP_LOGI(TAG, "Telemetry Payload Dispatched: %s", payload_string);
        }
        free(payload_string);
        cJSON_Delete(root);

        vTaskDelay(pdMS_TO_TICKS(TELEMETRY_INTERVAL_MS));
    }
}

static bool is_sensor_attached(int16_t raw_value) {
    if (raw_value == -9999) return false; 
    if (raw_value >= FLOATING_LEAK_MIN && raw_value <= FLOATING_LEAK_MAX) return false; 
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
            pdTRUE, pdFALSE,
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
        
        last_detailed_topology = current_detailed_topology;
        last_disco_publish = xTaskGetTickCount();

        cJSON *root = cJSON_CreateObject();
        cJSON_AddStringToObject(root, "t", "disco");
        cJSON_AddNumberToObject(root, "v", 1);
        cJSON_AddStringToObject(root, "tid", tenant_id);
        cJSON_AddStringToObject(root, "nid", node_id);
        cJSON_AddNumberToObject(root, "ts", (double)time(NULL));
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
            ESP_LOGW(TAG, "Discovery Packet Dispatched: %s", payload_string);
        }
        free(payload_string);
        cJSON_Delete(root);
    }
}

// ==========================================
// APPLICATION ENTRY POINT
// ==========================================
void app_main(void) {
    // 1. Initialize NVS Storage engine
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    // 2. Fetch NVS configuration parameters or enter AP mode if switch is grounded
    load_nvs_credentials();
    init_dynamic_identity();

    // 3. Create Mutexes & Event Groups
    i2c_mutex = xSemaphoreCreateMutex();
    data_mutex = xSemaphoreCreateMutex();
    task_tracking_mutex = xSemaphoreCreateMutex();
    s_hardware_event_group = xEventGroupCreate();

    init_actuators(); 

    if (i2c_master_init() != ESP_OK) {
        ESP_LOGE(TAG, "Failed to initialize I2C master peripheral engine.");
        vTaskSuspend(NULL);
    }

    scan_i2c_bus();

    // 4. Instantiate background FreeRTOS tasks
    xTaskCreate(ads_reader_task, "adc_worker", 3072, NULL, 5, NULL);
    xTaskCreate(telemetry_builder_task, "tlm_json", 4096, NULL, 5, NULL);
    xTaskCreate(discovery_builder_task, "disco_json", 4096, NULL, 5, NULL);
    xTaskCreate(cloud_ping_task, "cloud_ping", 3072, NULL, 3, NULL);

    // 5. Connect to active network interface and start MQTT state machine
    network_init();
}