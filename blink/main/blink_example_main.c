/**
 * Project: Environment-Agnostic IoT Monitoring Framework
 * Architecture: Cellular-First Routing with RTC Data Preservation & Active I2C Recovery
 * Hardware: ESP32-WROOM-32D, SIMCOM A7670C, Mini DS3231 RTC, ADS1115
 * Author: Korinne Margaret V. Sasil, Mikhail Alexi D. Hatulan
 * Institute: University of San Carlos, Talamban Campus
 *
 * Revision 2026-10-23:
 *   - Atomic ADS1115 acquisition with OS-bit conversion polling
 *   - PGA +/-6.144 V (0-5 V sensors no longer clip above 4.096 V)
 *   - SNTP-disciplined timekeeping; DS3231 written back after every sync
 *   - Payload timestamps from the system clock (no I2C traffic per message)
 *   - Non-blocking MQTT publishing for telemetry, discovery and ACKs
 *   - Command execution moved out of the MQTT task; duplicate redelivery dropped
 *   - Safe-eject quiesce waits for the in-flight transaction before gating
 *   - Auto-shutoff timers cancelled by notification, not vTaskDelete()
 *   - Offline spooling in both Wi-Fi and cellular modes
 *   - Actuator PWM at 1 kHz
 *
 * REQUIRED sdkconfig: CONFIG_FREERTOS_HZ=1000
 *   (menuconfig -> Component config -> FreeRTOS -> Kernel -> configTICK_RATE_HZ)
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
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
#include "freertos/queue.h"
#include "freertos/event_groups.h"

#include "esp_system.h"
#include "esp_mac.h"
#include "esp_rom_sys.h"
#include "esp_random.h"
#include "esp_timer.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_netif.h"
#include "esp_netif_ppp.h"
#include "esp_netif_sntp.h"
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

// The ADS1115 conversion poll needs 1 ms delay resolution. At the ESP-IDF
// default of 100 Hz, pdMS_TO_TICKS(1) rounds to 0 and the poll degenerates.
#if CONFIG_FREERTOS_HZ < 1000
#error "This firmware requires CONFIG_FREERTOS_HZ=1000 (menuconfig -> Component config -> FreeRTOS -> Kernel -> configTICK_RATE_HZ)"
#endif

static const char *TAG = "THESIS_NODE";

// pdMS_TO_TICKS() multiplies before dividing, so it overflows a 32-bit
// TickType_t above ~71 minutes at 1 kHz. Use this for anything that can be long
// (actuator durations up to 24 h, RTC re-seed intervals).
#define LONG_MS_TO_TICKS(ms)   ((TickType_t)((uint32_t)(ms) / portTICK_PERIOD_MS))

// ==========================================
// SYSTEM LED INDICATORS & SWITCHES
// ==========================================
#define SYSTEM_LED_NET            14
#define SYSTEM_LED_FAULT          23
#define EJECT_BUTTON_GPIO         34   // Safe Eject slide switch (input-only; external 10k pull-up)
#define EJECT_DEBOUNCE_MS         300  // switch must hold a new position this long to count
#define PROVISION_SWITCH_GPIO     32   // Captive-portal provisioning switch

// ==========================================
// TIMING & NETWORK TIMEOUT CONFIGURATIONS
// ==========================================
#define TELEMETRY_INTERVAL_MS               10000
#define DISCOVERY_HEARTBEAT_MS              60000
#define I2C_FAST_SCAN_INTERVAL_MS           3000
#define I2C_ZERO_CHIP_RECOVERY_COOLDOWN_MS  10000
#define HEALTH_LOG_EVERY_N_CYCLES           6      // one health line per minute

// --- Cloud & MQTT Timers ---
// How long a write to a dead link may block before the client gives up and
// disconnects. 60 s let a dropped hotspot go unnoticed for over a minute (and
// held the MQTT client lock the whole time); 20 s still covers a slow TLS
// handshake over cellular.
#define MQTT_NETWORK_TIMEOUT_MS             20000
// Mobile/hotspot NAT commonly drops idle mappings well before 120 s; the
// connection then dies silently and the next write hangs for the full network
// timeout. 45 s keeps the mapping alive.
#define MQTT_KEEPALIVE_SEC                  45
#define MAX_CLOUD_FAILURES                  3

// --- Wi-Fi Phase 2 Watchdog Timers ---
#define WIFI_WAN_PROBE_INTERVAL_MS          60000
#define WIFI_WAN_PROBE_TIMEOUT_SEC          3

// --- Cellular Modem Watchdog Timers ---
#define CELLULAR_BRINGUP_RETRY_MS           15000
#define CELLULAR_NET_WAIT_TIMEOUT_MS        60000
#define CELLULAR_PPP_DROP_TIMEOUT_SEC       30
#define CELLULAR_MQTT_OFFLINE_TIMEOUT_SEC   180

// --- Timekeeping ---
#define MIN_VALID_EPOCH            1704067200   // 2024-01-01T00:00:00Z; anything earlier is an unset clock
#define RTC_WRITE_RETRY_MS         10000
#define RTC_RESEED_INTERVAL_MS     3600000      // re-seed system clock from DS3231 hourly...
#define RTC_SNTP_STALE_MS          7200000      // ...but only when SNTP has not synced in 2 h

// --- Offline spool ---
#define SPOOL_FILE_PATH           "/fs/telemetry_spool.jsonl"
#define SPOOL_REPLAY_PATH         SPOOL_FILE_PATH ".replaying"
#define SPOOL_MAX_BYTES           1000000
#define REPLAY_WAIT_FOR_TIME_MS   90000   // hold replay up to 90 s for SNTP so pre-sync rows can be re-timed

// --- ADS1115 acquisition ---
// PGA = 000 -> FSR +/-6.144 V, 1 LSB = 187.5 uV, ~26,670 counts across 0-5 V.
// Config MSB per channel: OS=1 (start), MUX=1xx (AINx vs GND), PGA=000, MODE=1 (single-shot)
static const uint8_t ADS_MUX_MSB[4] = { 0xC1, 0xD1, 0xE1, 0xF1 };
#define ADS_CONFIG_LSB              0x83   // 128 SPS, comparator disabled
#define ADS_CONV_POLL_MAX_TRIES     25     // ms ceiling; 128 SPS needs ~7.8 ms
#define ADS_SUSTAINED_ERROR_LIMIT   8
// "Not read yet" (chip just appeared, or a port was just re-enabled). Never a real
// single-ended reading. Such ports are left out of telemetry instead of being
// sent as 0 V, which the backend would plot as a real dip to zero.
#define ADS_NO_DATA                 INT16_MIN

// --- Topology reporting ---
#define PORT_DEBOUNCE_SAMPLES       3      // agreeing scans before a change is believed

// --- Zero-chip recovery back-off ---
#define MAX_ZERO_CHIP_RECOVERIES    3
// A chip is declared gone only after this many consecutive failed probes, so a
// single NACK on a marginal bus no longer drops it (and its handle) for a scan.
#define PROBE_MISS_LIMIT            3
// When the bus itself is faulted (SDA/SCL held, RTC silent), recovery retries
// back off 10 s -> 20 s -> 40 s -> 80 s instead of hammering a dead bus forever.
#define I2C_FAULT_BACKOFF_MAX_MS    80000

// --- Command pipeline ---
#define MAX_CMD_PAYLOAD_LEN       1024
#define MAX_CID_LEN               64
#define CMD_QUEUE_DEPTH           16     // the web app can send a burst of 16 port commands at once
#define CMD_DEDUP_HISTORY         8
#define CMD_MAX_AGE_SEC           300    // reject commands issued more than 5 min ago

// ==========================================
// DYNAMIC TOPIC & ID BUFFERS
// ==========================================
char node_id[32];
char topic_tlm[128];
char topic_cmd[128];
char topic_ack[128];
char topic_disco[128];
char topic_status[128];
char topic_config[128];

// ==========================================
// ACTUATOR PIN MAP (final hardware pinout)
// ==========================================
#define NUM_ACTUATORS       8
const int actuator_gpios[NUM_ACTUATORS] = {4, 25, 13, 19, 26, 27, 33, 18};
const ledc_channel_t actuator_channels[NUM_ACTUATORS] = {
    LEDC_CHANNEL_0, LEDC_CHANNEL_1, LEDC_CHANNEL_2, LEDC_CHANNEL_3,
    LEDC_CHANNEL_4, LEDC_CHANNEL_5, LEDC_CHANNEL_6, LEDC_CHANNEL_7
};
#define ACTUATOR_LEDC_MODE          LEDC_LOW_SPEED_MODE
#define ACTUATOR_LEDC_TIMER         LEDC_TIMER_0
#define ACTUATOR_LEDC_RES           LEDC_TIMER_8_BIT
#define ACTUATOR_LEDC_FREQ          1000              // 1 kHz: smooth proportional output
#define ACTUATOR_DUTY_FULL          (1 << 8)          // 2^res = true 100% (255 alone is 99.6%)

typedef struct {
    int  target_idx;
    int  duration_ms;
    char cid[MAX_CID_LEN];
} auto_shutoff_args_t;

TaskHandle_t auto_shutoff_task_handles[NUM_ACTUATORS] = {NULL};

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
volatile bool safe_eject_active = false;

// ==========================================
// DYNAMIC CREDENTIAL ARRAYS
// ==========================================
char wifi_ssid[64] = DEFAULT_WIFI_SSID;
char wifi_pass[64] = DEFAULT_WIFI_PASS;
char cellular_apn[64] = CELLULAR_APN;
char pri_broker_uri[128] = MQTT_BROKER_URI;
char sec_broker_uri[128] = SEC_BROKER_URI;
char tenant_id[32] = DEFAULT_TENANT_ID;

// ==========================================
// I2C & HARDWARE GLOBALS
// ==========================================
#define I2C_MASTER_SDA_IO   21
#define I2C_MASTER_SCL_IO   22
#define TCA4307_EN_GPIO     15   // LLC-boosted hardware gate (harmless if the TCA4307 is bypassed)
#define I2C_MASTER_FREQ_HZ  100000
// I2C ISR IRAM-safe lets the I2C interrupt keep running while flash is being
// written. Enable in menuconfig (I2C driver options, "ISR IRAM-safe"); the
// option name can differ between ESP-IDF versions.
// (Reported at boot as a log line; a compile-time #warning breaks -Werror builds.)
#if defined(CONFIG_I2C_ISR_IRAM_SAFE) && CONFIG_I2C_ISR_IRAM_SAFE
#define I2C_IRAM_SAFE_ENABLED 1
#else
#define I2C_IRAM_SAFE_ENABLED 0
#endif

// Per-transfer timeout. A healthy transfer completes in under 1 ms and an absent
// device NACKs immediately, so this only bounds a FAILING transfer -- and that
// bound is how long one bad board can hold the bus from every other board.
// Without IRAM-safe, a flash erase (LittleFS spooling) can stall the ISR for
// hundreds of ms, so 500 ms is needed; with it, 100 ms is safe and a bad board
// costs 5x less bus time.
#if I2C_IRAM_SAFE_ENABLED
#define I2C_XFER_TIMEOUT_MS 100
#else
#define I2C_XFER_TIMEOUT_MS 500
#endif
#define DS3231_ADDR         0x68
#define DS3231_REG_TIME     0x00
#define DS3231_REG_STATUS   0x0F
#define DS3231_STATUS_OSF   0x80   // Oscillator Stop Flag: set if the RTC ever lost power
#define DS3231_REG_CONTROL  0x0E
#define DS3231_CONTROL_EOSC 0x80   // 1 = oscillator stops while on battery
#define REG_POINTER_CONVERT 0x00
#define REG_POINTER_CONFIG  0x01

i2c_master_bus_handle_t bus_handle = NULL;
i2c_master_dev_handle_t ds3231_handle = NULL;
i2c_master_dev_handle_t ads_handles[4] = {NULL, NULL, NULL, NULL};
const uint8_t possible_addresses[4] = {0x48, 0x49, 0x4A, 0x4B};

uint8_t num_ads_found = 0;
static TickType_t last_zero_chip_recovery_tick = 0;

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

// ==========================================
// COMMAND PIPELINE GLOBALS
// ==========================================
typedef struct {
    size_t len;
    char   payload[MAX_CMD_PAYLOAD_LEN];
} cmd_msg_t;

static QueueHandle_t command_queue = NULL;

// Telemetry rows (heap strings) from the builder to the publisher. 4 rows = 40 s;
// beyond that the builder spools to flash instead of waiting.
#define TLM_OUT_QUEUE_DEPTH 4
static QueueHandle_t tlm_out_queue = NULL;

// The spool file now has two writers (builder and publisher) plus the replay's
// rename; all three go through this lock.
static SemaphoreHandle_t spool_mutex = NULL;

// Serialises configure_mqtt_client(): the Wi-Fi event handler, the cloud
// watchdog and the cellular task can all call it.
static SemaphoreHandle_t mqtt_cfg_mutex = NULL;

// Staging buffer for the MQTT event handler. Safe as a single static because
// MQTT_EVENT_DATA is only ever dispatched from the MQTT task, and xQueueSend
// copies it out before returning. Keeps 1 KB off that task's stack.
static cmd_msg_t cmd_staging;

// ==========================================
// TIMEKEEPING GLOBALS
// ==========================================
static volatile bool sntp_sync_pending = false;

// Distinguishes this boot from earlier ones, so a spooled row's uptime can only
// be converted back to wall-clock time within the boot that recorded it.
static uint32_t boot_id = 0;

// ==========================================
// TENANT HANDOVER GLOBALS
// ==========================================
// Set at boot when the provisioned tenant differs from the one this node last
// completed a connection under (NVS key "last_tid"), or when no history exists.
static char previous_tenant_id[32] = "";   // empty when there is no old tenant to close out
static volatile bool handover_pending = false;
static bool broker_session_purge = false;   // connect with a clean session this boot

// ==========================================
// FORWARD DECLARATIONS
// ==========================================
static void configure_mqtt_client(void);
static esp_err_t i2c_master_init(void);
static void recover_i2c_bus(void);
static void send_command_ack(const char *cid, const char *action, const char *status, const char *details);

// ==========================================
// STATUS LED & HARDWARE GATE INITIALIZATION
// ==========================================
static void init_system_leds(void) {
    gpio_config_t led_cfg = {
        .pin_bit_mask = (1ULL << SYSTEM_LED_NET) | (1ULL << SYSTEM_LED_FAULT) | (1ULL << TCA4307_EN_GPIO),
        .mode = GPIO_MODE_OUTPUT,
        .pull_up_en = GPIO_PULLUP_DISABLE,
        .pull_down_en = GPIO_PULLDOWN_DISABLE,
        .intr_type = GPIO_INTR_DISABLE
    };
    gpio_config(&led_cfg);
    gpio_set_level(SYSTEM_LED_NET, 0);
    gpio_set_level(SYSTEM_LED_FAULT, 0);
    gpio_set_level(TCA4307_EN_GPIO, 1);   // gate ON by default
}

// Network LED: solid when MQTT is connected, 2 Hz blink when not.
// Fault LED while safe eject is active: slow 1 Hz blink, so it can't be confused
// with the solid "no ADS1115 detected" fault. Outside eject, the I2C scan owns it.
void network_led_task(void *pvParameter) {
    uint32_t tick = 0;
    while (1) {
        tick++;
        gpio_set_level(SYSTEM_LED_NET, is_mqtt_connected ? 1 : (tick & 1));
        if (safe_eject_active) gpio_set_level(SYSTEM_LED_FAULT, (tick >> 1) & 1);
        vTaskDelay(pdMS_TO_TICKS(250));
    }
}

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
    sprintf(topic_config, "%s/%s/%s/config", MQTT_TOPIC_ROOT, tenant_id, node_id);

    ESP_LOGI(TAG, "====================================");
    ESP_LOGI(TAG, "PROVISIONED AS: %s / %s", tenant_id, node_id);
    ESP_LOGI(TAG, "Command Topic: %s", topic_cmd);
    ESP_LOGI(TAG, "Telemetry Topic: %s", topic_tlm);
    ESP_LOGI(TAG, "====================================");
}

// ==========================================
// TIMEKEEPING (DS3231 + SNTP)
// ==========================================
// Payload timestamps come from the system clock. The system clock is seeded
// from the DS3231 at boot, disciplined by SNTP whenever a network is available,
// and re-seeded from the DS3231 hourly when SNTP is not. The DS3231 is written
// back after every SNTP sync so the correct time survives reboots and outages.
// Net effect: no I2C transaction per payload, no I2C access from the MQTT task,
// and no bus traffic while a pod is being swapped.

static uint8_t bcd2dec(uint8_t v) { return (uint8_t)((v >> 4) * 10 + (v & 0x0F)); }
static uint8_t dec2bcd(uint8_t v) { return (uint8_t)(((v / 10) << 4) | (v % 10)); }

static time_t node_epoch(void) { return time(NULL); }

// Returns 0 on failure. Optionally reports the Oscillator Stop Flag.
// A read can "succeed" on a noisy bus and still return garbage (seen in the
// field: year 2167). Reject anything that is not valid BCD or is out of range,
// so a corrupted read can never set the system clock or stamp telemetry.
static bool bcd_ok(uint8_t v, uint8_t mask, uint8_t lo, uint8_t hi) {
    v &= mask;
    if ((v & 0x0F) > 9 || (v >> 4) > 9) return false;
    uint8_t d = bcd2dec(v);
    return d >= lo && d <= hi;
}
static bool ds3231_fields_valid(const uint8_t *d) {
    if (d[2] & 0x40) return false;                       // 12-hour mode is never written by this firmware
    if (d[5] & 0x80) return false;                       // century bit: this firmware only writes 20xx (caught the 2167 reads)
    // ponytail: year capped at 2035 (a corrupt read gave 2046); raise the 35 if still deployed then
    return bcd_ok(d[0], 0x7F, 0, 59) && bcd_ok(d[1], 0x7F, 0, 59) && bcd_ok(d[2], 0x3F, 0, 23) &&
           bcd_ok(d[3], 0x07, 1, 7)  && bcd_ok(d[4], 0x3F, 1, 31) && bcd_ok(d[5], 0x1F, 1, 12) &&
           bcd_ok(d[6], 0xFF, 24, 35);
}

static time_t ds3231_read_epoch(bool *osf_set) {
    if (osf_set) *osf_set = false;
    if (ds3231_handle == NULL) return 0;

    uint8_t reg = DS3231_REG_TIME;
    uint8_t data[7] = {0};
    time_t epoch = 0;

    if (xSemaphoreTake(i2c_mutex, pdMS_TO_TICKS(200)) == pdTRUE) {
        if (ds3231_handle != NULL &&
            i2c_master_transmit_receive(ds3231_handle, &reg, 1, data, sizeof(data), I2C_XFER_TIMEOUT_MS) == ESP_OK &&
            ds3231_fields_valid(data)) {
            struct tm t = {0};
            t.tm_sec  = bcd2dec(data[0] & 0x7F);
            t.tm_min  = bcd2dec(data[1] & 0x7F);
            t.tm_hour = bcd2dec(data[2] & 0x3F);     // 24-hour mode
            t.tm_mday = bcd2dec(data[4] & 0x3F);
            t.tm_mon  = bcd2dec(data[5] & 0x1F) - 1;
            t.tm_year = bcd2dec(data[6]) + 100;      // 2000-2099
            t.tm_isdst = 0;
            epoch = mktime(&t);                      // TZ is forced to UTC in app_main
        }
        if (osf_set && ds3231_handle != NULL) {
            uint8_t sreg = DS3231_REG_STATUS, status = 0;
            if (i2c_master_transmit_receive(ds3231_handle, &sreg, 1, &status, 1, I2C_XFER_TIMEOUT_MS) == ESP_OK) {
                *osf_set = (status & DS3231_STATUS_OSF) != 0;
            }
        }
        xSemaphoreGive(i2c_mutex);
    }
    return (epoch > 0) ? epoch : 0;
}

static bool ds3231_write_epoch(time_t now) {
    if (ds3231_handle == NULL) return false;

    struct tm t;
    gmtime_r(&now, &t);
    if (t.tm_year < 100 || t.tm_year > 199) return false;   // DS3231 holds 2000-2099 only

    uint8_t buf[8] = {
        DS3231_REG_TIME,
        dec2bcd((uint8_t)t.tm_sec),
        dec2bcd((uint8_t)t.tm_min),
        dec2bcd((uint8_t)t.tm_hour),          // bit 6 = 0 -> 24-hour mode
        dec2bcd((uint8_t)(t.tm_wday + 1)),    // DS3231 day-of-week is 1-7
        dec2bcd((uint8_t)t.tm_mday),
        dec2bcd((uint8_t)(t.tm_mon + 1)),
        dec2bcd((uint8_t)(t.tm_year - 100))
    };

    bool ok = false;
    if (xSemaphoreTake(i2c_mutex, pdMS_TO_TICKS(200)) == pdTRUE) {
        if (ds3231_handle != NULL &&
            i2c_master_transmit(ds3231_handle, buf, sizeof(buf), I2C_XFER_TIMEOUT_MS) == ESP_OK) {
            ok = true;
            // Clear the Oscillator Stop Flag: the time is now known-good.
            uint8_t sreg = DS3231_REG_STATUS, status = 0;
            if (i2c_master_transmit_receive(ds3231_handle, &sreg, 1, &status, 1, I2C_XFER_TIMEOUT_MS) == ESP_OK &&
                (status & DS3231_STATUS_OSF)) {
                uint8_t clr[2] = { DS3231_REG_STATUS, (uint8_t)(status & ~DS3231_STATUS_OSF) };
                i2c_master_transmit(ds3231_handle, clr, sizeof(clr), I2C_XFER_TIMEOUT_MS);
            }
            // EOSC (control bit 7) set = oscillator stops whenever the RTC runs on
            // its battery, i.e. time freezes while the board is unpowered. It is 0
            // from the factory, but a corrupted write on the faulty bus can set it.
            uint8_t creg = DS3231_REG_CONTROL, ctrl = 0;
            if (i2c_master_transmit_receive(ds3231_handle, &creg, 1, &ctrl, 1, I2C_XFER_TIMEOUT_MS) == ESP_OK &&
                (ctrl & DS3231_CONTROL_EOSC)) {
                uint8_t fix[2] = { DS3231_REG_CONTROL, (uint8_t)(ctrl & ~DS3231_CONTROL_EOSC) };
                i2c_master_transmit(ds3231_handle, fix, sizeof(fix), I2C_XFER_TIMEOUT_MS);
                ESP_LOGW(TAG, "DS3231 oscillator was set to stop on battery (EOSC=1); cleared.");
            }
        }
        xSemaphoreGive(i2c_mutex);
    }
    return ok;
}

static void seed_system_time_from_ds3231(bool at_boot) {
    bool osf = false;
    time_t rtc = ds3231_read_epoch(&osf);
    if (rtc <= 0) {
        ESP_LOGE(TAG, "DS3231 read failed or returned an invalid time; system clock not seeded.");
        return;
    }
    struct timeval tv = { .tv_sec = rtc, .tv_usec = 0 };
    settimeofday(&tv, NULL);

    if (at_boot) {
        char iso[32];
        struct tm t; gmtime_r(&rtc, &t);
        strftime(iso, sizeof(iso), "%Y-%m-%dT%H:%M:%SZ", &t);
        ESP_LOGI(TAG, "System clock seeded from DS3231: %s", iso);
        // OSF = the oscillator stopped at some point, so the time is behind by
        // however long it was stopped (seen in the field: 27 s after a power
        // cycle). Still used: on the local edge with no internet there is no
        // SNTP, and a slightly late timestamp beats an undatable row.
        if (osf) {
            ESP_LOGW(TAG, "DS3231 oscillator stopped since its last sync (OSF=1): time may be behind until SNTP. "
                          "Check the RTC backup battery.");
        }
    }
}

// Runs in the lwIP/SNTP context: only set a flag. The DS3231 write happens in
// time_sync_task, where blocking on the I2C mutex is acceptable.
static void on_sntp_sync(struct timeval *tv) {
    (void)tv;
    sntp_sync_pending = true;
}

void time_sync_task(void *pvParameter) {
    bool have_sntp = false;
    TickType_t last_sntp_tick = 0;
    TickType_t last_reseed_tick = xTaskGetTickCount();
    TickType_t last_write_attempt = 0;

    while (1) {
        TickType_t now_tick = xTaskGetTickCount();

        // 1. SNTP delivered fresh time: persist it to the DS3231.
        if (sntp_sync_pending && !safe_eject_active &&
            (now_tick - last_write_attempt) >= pdMS_TO_TICKS(RTC_WRITE_RETRY_MS)) {
            last_write_attempt = now_tick;
            time_t now = time(NULL);
            if (now >= MIN_VALID_EPOCH && ds3231_write_epoch(now)) {
                sntp_sync_pending = false;
                have_sntp = true;
                last_sntp_tick = now_tick;
                char iso[32];
                struct tm t; gmtime_r(&now, &t);
                strftime(iso, sizeof(iso), "%Y-%m-%dT%H:%M:%SZ", &t);
                ESP_LOGI(TAG, "SNTP sync complete. DS3231 updated to %s", iso);
            } else {
                ESP_LOGW(TAG, "SNTP time received but DS3231 write failed; retrying in %d s.",
                         RTC_WRITE_RETRY_MS / 1000);
            }
        }

        // 2. No recent SNTP (offline / cellular blackout): the DS3231 drifts
        //    far less than the ESP32's own clock, so re-seed from it hourly.
        bool sntp_stale = !have_sntp ||
                          (now_tick - last_sntp_tick) > LONG_MS_TO_TICKS(RTC_SNTP_STALE_MS);
        if (sntp_stale && !safe_eject_active &&
            (now_tick - last_reseed_tick) > LONG_MS_TO_TICKS(RTC_RESEED_INTERVAL_MS)) {
            last_reseed_tick = now_tick;
            seed_system_time_from_ds3231(false);
        }

        vTaskDelay(pdMS_TO_TICKS(1000));
    }
}

// ==========================================
// TWO-STEP ACKNOWLEDGEMENT LOGIC
// ==========================================
static void send_command_ack(const char *cid, const char *action, const char *status, const char *details) {
    if (!is_mqtt_connected || mqtt_client == NULL) return;

    cJSON *ack_root = cJSON_CreateObject();
    if (ack_root == NULL) return;

    cJSON_AddStringToObject(ack_root, "t", "ack");
    cJSON_AddNumberToObject(ack_root, "v", 1);
    cJSON_AddStringToObject(ack_root, "tid", tenant_id);
    cJSON_AddStringToObject(ack_root, "nid", node_id);
    cJSON_AddStringToObject(ack_root, "cid", cid ? cid : "unknown");
    cJSON_AddStringToObject(ack_root, "action", action ? action : "unknown");
    cJSON_AddStringToObject(ack_root, "status", status);
    cJSON_AddStringToObject(ack_root, "details", details ? details : "");
    cJSON_AddNumberToObject(ack_root, "ts", (double)node_epoch());

    char *payload = cJSON_PrintUnformatted(ack_root);
    if (payload != NULL) {
        // enqueue, not publish: never block the calling task on the network.
        if (esp_mqtt_client_enqueue(mqtt_client, topic_ack, payload, 0, 1, 0, true) < 0) {
            ESP_LOGW(TAG, "ACK enqueue failed for ID %s", cid ? cid : "unknown");
        } else {
            ESP_LOGI(TAG, "Command ACK published -> Status: %s | Action: %s | ID: %s",
                     status, action ? action : "unknown", cid ? cid : "unknown");
        }
        free(payload);
    }
    cJSON_Delete(ack_root);
}

// ==========================================
// ACTUATORS
// ==========================================
static void init_actuators(void) {
    ledc_timer_config_t ledc_timer = {
        .speed_mode      = ACTUATOR_LEDC_MODE,
        .timer_num       = ACTUATOR_LEDC_TIMER,
        .duty_resolution = ACTUATOR_LEDC_RES,
        .freq_hz         = ACTUATOR_LEDC_FREQ,
        .clk_cfg         = LEDC_AUTO_CLK
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
    ESP_LOGI(TAG, "Actuator outputs OUT1-OUT%d ready (PWM %d Hz, 8-bit).", NUM_ACTUATORS, ACTUATOR_LEDC_FREQ);
}

static void actuator_force_off(int idx) {
    ledc_stop(ACTUATOR_LEDC_MODE, actuator_channels[idx], 0);
}

// Auto-shutoff timers are cancelled by task notification, never vTaskDelete().
// Deleting a task that is mid-ACK can kill it while it holds the MQTT client's
// internal lock, deadlocking the client permanently. An ownership check under
// task_tracking_mutex also guarantees that a timer which expires at the same
// moment a new command arrives can never switch off the freshly commanded output.
void auto_shutoff_task(void *pvParameter) {
    auto_shutoff_args_t args = *(auto_shutoff_args_t *)pvParameter;
    free(pvParameter);

    uint32_t cancelled = ulTaskNotifyTake(pdTRUE, LONG_MS_TO_TICKS(args.duration_ms));

    bool owner = false;
    if (xSemaphoreTake(task_tracking_mutex, portMAX_DELAY) == pdTRUE) {
        if (!cancelled && auto_shutoff_task_handles[args.target_idx] == xTaskGetCurrentTaskHandle()) {
            owner = true;
            auto_shutoff_task_handles[args.target_idx] = NULL;
            actuator_force_off(args.target_idx);
        }
        xSemaphoreGive(task_tracking_mutex);
    }

    if (owner) {
        ESP_LOGW(TAG, ">>> Auto-shutoff triggered for OUT%d after %d ms <<<", args.target_idx + 1, args.duration_ms);
        send_command_ack(args.cid, "actuate", "completed", "Auto-shutoff execution window expired safely");
    }
    vTaskDelete(NULL);
}

static void cancel_auto_shutoff(int idx) {
    if (xSemaphoreTake(task_tracking_mutex, portMAX_DELAY) == pdTRUE) {
        if (auto_shutoff_task_handles[idx] != NULL) {
            xTaskNotifyGive(auto_shutoff_task_handles[idx]);
            auto_shutoff_task_handles[idx] = NULL;
            ESP_LOGW(TAG, "Cancelled pending auto-shutoff on OUT%d (superseded by new command).", idx + 1);
        }
        xSemaphoreGive(task_tracking_mutex);
    }
}

static bool arm_auto_shutoff(int idx, int duration_ms, const char *cid) {
    auto_shutoff_args_t *args = malloc(sizeof(auto_shutoff_args_t));
    if (args == NULL) return false;
    args->target_idx = idx;
    args->duration_ms = duration_ms;
    snprintf(args->cid, sizeof(args->cid), "%s", cid);

    bool ok = false;
    if (xSemaphoreTake(task_tracking_mutex, portMAX_DELAY) == pdTRUE) {
        if (xTaskCreate(auto_shutoff_task, "auto_shutoff", 3072, args, 5,
                        &auto_shutoff_task_handles[idx]) == pdPASS) {
            ok = true;
        } else {
            auto_shutoff_task_handles[idx] = NULL;
        }
        xSemaphoreGive(task_tracking_mutex);
    }
    if (!ok) free(args);
    return ok;
}

// ==========================================
// LITTLEFS OFFLINE STORAGE
// ==========================================
static void init_littlefs(void) {
    esp_vfs_littlefs_conf_t conf = {
        .base_path = "/fs", .partition_label = "spiffs", .format_if_mount_failed = true, .dont_mount = false,
    };
    if (esp_vfs_littlefs_register(&conf) == ESP_OK) ESP_LOGI(TAG, "LittleFS mounted for offline buffering.");
    else ESP_LOGE(TAG, "LittleFS mount failed; offline buffering unavailable.");
}

static double uptime_sec(void) {
    return (double)esp_timer_get_time() / 1000000.0;
}

// Tenant IDs end up inside MQTT topic strings. Only allow the characters the
// captive portal permits, so a corrupted spool row can never inject '/', '+'
// or '#' and publish somewhere it shouldn't.
static bool tenant_id_is_safe(const char *tid) {
    if (tid == NULL) return false;
    size_t n = strnlen(tid, 32);
    if (n == 0 || n >= 32) return false;
    for (size_t i = 0; i < n; i++) {
        char c = tid[i];
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-')) return false;
    }
    return true;
}

static void spool_append_line(const char *payload) {
    xSemaphoreTake(spool_mutex, portMAX_DELAY);
    struct stat st;
    if (stat(SPOOL_FILE_PATH, &st) == 0 && st.st_size > SPOOL_MAX_BYTES) {
        ESP_LOGE(TAG, "Spool reached %d bytes. Wiping backlog to protect flash.", SPOOL_MAX_BYTES);
        remove(SPOOL_FILE_PATH);
    }
    FILE *f = fopen(SPOOL_FILE_PATH, "a");
    bool ok = false;
    if (f != NULL) {
        fprintf(f, "%s\n", payload);
        fclose(f);
        ok = true;
    }
    xSemaphoreGive(spool_mutex);
    if (ok) ESP_LOGW(TAG, "[OFFLINE] Buffered to LittleFS -> %s", payload);
    else    ESP_LOGE(TAG, "Spool open failed (%s). Telemetry dropped.", SPOOL_FILE_PATH);
}

// Spools a telemetry object. The backend drops any late row whose timestamp is
// before 2026 as "undatable", so if the clock is not yet valid the row also
// records uptime ("_up") and this boot's ID ("_b"). The replay uses them to
// re-time the row once SNTP has synced, then strips them before publishing.
static void spool_telemetry_root(cJSON *root) {
    if (node_epoch() < MIN_VALID_EPOCH) {
        cJSON_AddNumberToObject(root, "_up", uptime_sec());
        cJSON_AddNumberToObject(root, "_b", (double)boot_id);
    }
    char *s = cJSON_PrintUnformatted(root);
    if (s != NULL) {
        spool_append_line(s);
        free(s);
    }
}

// ==========================================
// I2C BUS INIT & RECOVERY
// ==========================================
static esp_err_t i2c_master_init(void) {
    i2c_master_bus_config_t i2c_mst_config = {
        .clk_source = I2C_CLK_SRC_DEFAULT,
        .i2c_port = I2C_NUM_0,
        .scl_io_num = I2C_MASTER_SCL_IO,
        .sda_io_num = I2C_MASTER_SDA_IO,
        .glitch_ignore_cnt = 7,
        .flags.enable_internal_pullup = true,
    };
    return i2c_new_master_bus(&i2c_mst_config, &bus_handle);
}

// Releases a slave left holding SDA low -- e.g. a battery-backed DS3231 that was
// mid-read when the ESP32 reset, or a transfer aborted by a timeout: 9 clocks +
// STOP, open-drain, ~100 kHz. The I2C driver must not own the pins while it runs.
static void i2c_bus_clear(void) {
    // Latch the outputs HIGH before enabling them, so enabling open-drain mode
    // cannot pull SDA/SCL low for a moment (a spurious START on the bus).
    gpio_set_level(I2C_MASTER_SDA_IO, 1);
    gpio_set_level(I2C_MASTER_SCL_IO, 1);
    gpio_config_t bb_cfg = {
        .pin_bit_mask = (1ULL << I2C_MASTER_SDA_IO) | (1ULL << I2C_MASTER_SCL_IO),
        .mode = GPIO_MODE_INPUT_OUTPUT_OD,
        .pull_up_en = GPIO_PULLUP_ENABLE,
        .pull_down_en = GPIO_PULLDOWN_DISABLE,
        .intr_type = GPIO_INTR_DISABLE
    };
    gpio_config(&bb_cfg);
    esp_rom_delay_us(5);
    for (int i = 0; i < 9; i++) {
        gpio_set_level(I2C_MASTER_SCL_IO, 0); esp_rom_delay_us(5);
        gpio_set_level(I2C_MASTER_SCL_IO, 1); esp_rom_delay_us(5);
    }
    // STOP: SDA rises while SCL is high
    gpio_set_level(I2C_MASTER_SCL_IO, 0); esp_rom_delay_us(5);
    gpio_set_level(I2C_MASTER_SDA_IO, 0); esp_rom_delay_us(5);
    gpio_set_level(I2C_MASTER_SCL_IO, 1); esp_rom_delay_us(5);
    gpio_set_level(I2C_MASTER_SDA_IO, 1); esp_rom_delay_us(5);

    gpio_reset_pin(I2C_MASTER_SDA_IO);
    gpio_reset_pin(I2C_MASTER_SCL_IO);
}

static void recover_i2c_bus(void) {
    // Never reset the bus or touch the gate while a pod may be mid-swap.
    if (safe_eject_active) {
        ESP_LOGW(TAG, "I2C recovery skipped: safe eject is active.");
        return;
    }

    ESP_LOGW(TAG, "Executing structured hardware + software I2C recovery routine...");

    // Synchronise the cooldown globally so scan_i2c_bus() and ads_reader_task()
    // never trigger back-to-back redundant resets.
    last_zero_chip_recovery_tick = xTaskGetTickCount();

    if (xSemaphoreTake(i2c_mutex, portMAX_DELAY) == pdTRUE) {
        // 1. Tear down ESP-IDF handles
        for (int i = 0; i < 4; i++) {
            if (ads_handles[i] != NULL) { i2c_master_bus_rm_device(ads_handles[i]); ads_handles[i] = NULL; }
        }
        if (ds3231_handle != NULL) { i2c_master_bus_rm_device(ds3231_handle); ds3231_handle = NULL; }
        if (bus_handle != NULL) { i2c_del_master_bus(bus_handle); bus_handle = NULL; }

        // 2. Cycle the hardware gate (no-op if the TCA4307 is bypassed)
        ESP_LOGW(TAG, "Cycling bus gate EN (GPIO %d) LOW -> HIGH...", TCA4307_EN_GPIO);
        gpio_set_level(TCA4307_EN_GPIO, 0);
        vTaskDelay(pdMS_TO_TICKS(25));
        gpio_set_level(TCA4307_EN_GPIO, 1);
        vTaskDelay(pdMS_TO_TICKS(25));

        // 3. Release any slave holding SDA low
        i2c_bus_clear();

        // 4. Re-initialise the master and restore the RTC
        if (i2c_master_init() == ESP_OK) {
            i2c_device_config_t ds_cfg = {
                .dev_addr_length = I2C_ADDR_BIT_LEN_7,
                .device_address = DS3231_ADDR,
                .scl_speed_hz = I2C_MASTER_FREQ_HZ
            };
            i2c_master_bus_add_device(bus_handle, &ds_cfg, &ds3231_handle);
            ESP_LOGI(TAG, "I2C master, bus gate and RTC restored.");
        } else {
            ESP_LOGE(TAG, "Fatal fault re-instantiating the I2C master bus.");
        }
        xSemaphoreGive(i2c_mutex);
    }
}

// ==========================================
// NON-DESTRUCTIVE I2C SCANNING
// ==========================================
// Returns the number of ADS1115s present, -1 if the bus mutex was busy, or -2 if
// the bus itself is faulted. -1 must NOT be treated as "zero chips": a busy bus
// is not an empty bus. -2 is detected from the RTC probe: a TIMEOUT (as opposed
// to a NACK) means SDA/SCL are being held, so every other probe would also burn
// the full timeout -- skip them and count the pass as a miss for every chip.
static uint8_t probe_misses[4] = {0};

// i2c_master_probe() always runs at a hard-coded 100 kHz, ignoring I2C_MASTER_FREQ_HZ.
// This probe uses a throwaway device at our bus speed and reads one register.
// ESP_ERR_TIMEOUT here means SDA or SCL is still physically low after the failure.
static esp_err_t probe_addr(uint16_t addr) {
    i2c_master_dev_handle_t h = NULL;
    i2c_device_config_t cfg = {
        .dev_addr_length = I2C_ADDR_BIT_LEN_7,
        .device_address  = addr,
        .scl_speed_hz    = I2C_MASTER_FREQ_HZ
    };
    if (i2c_master_bus_add_device(bus_handle, &cfg, &h) != ESP_OK) return ESP_FAIL;
    uint8_t reg = (addr == DS3231_ADDR) ? DS3231_REG_STATUS : 0x01, b = 0;   // ADS1115: config register
    esp_err_t e = i2c_master_transmit_receive(h, &reg, 1, &b, 1, I2C_XFER_TIMEOUT_MS);
    i2c_master_bus_rm_device(h);
    if (e != ESP_OK && (gpio_get_level(I2C_MASTER_SDA_IO) == 0 || gpio_get_level(I2C_MASTER_SCL_IO) == 0)) {
        return ESP_ERR_TIMEOUT;
    }
    return e;
}

static int perform_i2c_probe_pass(bool *rtc_ok) {
    *rtc_ok = false;
    if (xSemaphoreTake(i2c_mutex, pdMS_TO_TICKS(200)) != pdTRUE) return -1;

    bool bus_fault = (bus_handle == NULL);
    if (!bus_fault) {
        esp_err_t rtc_err = probe_addr(DS3231_ADDR);
        *rtc_ok = (rtc_err == ESP_OK);
        bus_fault = (rtc_err == ESP_ERR_TIMEOUT);
    }

    bool online[4];
    int found = 0;
    for (int i = 0; i < 4; i++) {
        if (safe_eject_active) {             // switch flipped mid-pass: results would be meaningless
            xSemaphoreGive(i2c_mutex);
            return -1;
        }
        bool ack = !bus_fault &&
                   probe_addr(possible_addresses[i]) == ESP_OK;
        if (ack) probe_misses[i] = 0;
        else if (probe_misses[i] < PROBE_MISS_LIMIT) probe_misses[i]++;

        // A chip that is already online survives isolated misses; it is dropped
        // only after PROBE_MISS_LIMIT consecutive misses.
        online[i] = ack || (ads_handles[i] != NULL && probe_misses[i] < PROBE_MISS_LIMIT);

        if (online[i] && ads_handles[i] == NULL && !bus_fault) {
            i2c_device_config_t dev_cfg = {
                .dev_addr_length = I2C_ADDR_BIT_LEN_7,
                .device_address = possible_addresses[i],
                .scl_speed_hz = I2C_MASTER_FREQ_HZ
            };
            if (i2c_master_bus_add_device(bus_handle, &dev_cfg, &ads_handles[i]) != ESP_OK) {
                ads_handles[i] = NULL;
                online[i] = false;
            }
        } else if (!online[i] && ads_handles[i] != NULL) {
            i2c_master_bus_rm_device(ads_handles[i]);
            ads_handles[i] = NULL;
        }
        if (online[i]) found++;
    }
    xSemaphoreGive(i2c_mutex);

    // Bookkeeping only: data_mutex is no longer held across I2C probes, so
    // telemetry is never blocked behind a slow or faulted bus.
    if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
        for (int i = 0; i < 4; i++) {
            global_node_data[i].address = possible_addresses[i];
            if (!online[i] || !global_node_data[i].is_online) {   // offline, or just came online
                for (int ch = 0; ch < 4; ch++) global_node_data[i].port_values[ch] = ADS_NO_DATA;
            }
            global_node_data[i].is_online = online[i];
        }
        num_ads_found = (uint8_t)found;
        if (!safe_eject_active) {
            gpio_set_level(SYSTEM_LED_FAULT, (num_ads_found == 0) ? 1 : 0);
        }
        xSemaphoreGive(data_mutex);
    }
    return bus_fault ? -2 : found;
}

static void scan_i2c_bus(void) {
    if (safe_eject_active) return;

    static uint8_t zero_chip_recovery_attempts = 0;
    static uint32_t fault_cooldown_ms = I2C_ZERO_CHIP_RECOVERY_COOLDOWN_MS;

    bool rtc_ok = false;
    int found = perform_i2c_probe_pass(&rtc_ok);

    if (found == -1 || safe_eject_active) return;       // bus busy or eject began mid-pass; try again next cycle
    if (found > 0 || rtc_ok) fault_cooldown_ms = I2C_ZERO_CHIP_RECOVERY_COOLDOWN_MS;  // bus works
    if (found > 0) { zero_chip_recovery_attempts = 0; return; }

    // Zero chips. Either the bus is healthy and simply has no pods (RTC answers),
    // or the bus is faulted (probe timed out, or the RTC is silent too).
    bool bus_fault = (found == -2) || !rtc_ok;

    TickType_t now = xTaskGetTickCount();
    uint32_t cooldown_ms = bus_fault ? fault_cooldown_ms : I2C_ZERO_CHIP_RECOVERY_COOLDOWN_MS;
    if ((now - last_zero_chip_recovery_tick) < pdMS_TO_TICKS(cooldown_ms)) return;

    // Healthy bus, no pods: a few attempts (the gate can latch), then stop.
    // perform_i2c_probe_pass() still runs every scan, so a pod plugged in later
    // is discovered and resets the counter.
    if (!bus_fault && zero_chip_recovery_attempts >= MAX_ZERO_CHIP_RECOVERIES) return;

    last_zero_chip_recovery_tick = now;
    if (bus_fault) {
        ESP_LOGW(TAG, "I2C bus fault (%s). Recovery now; next attempt in %lu s if it persists.",
                 found == -2 ? "SDA/SCL held: probe timed out" : "RTC not answering",
                 (unsigned long)((fault_cooldown_ms < I2C_FAULT_BACKOFF_MAX_MS ? fault_cooldown_ms * 2 : fault_cooldown_ms) / 1000));
        if (fault_cooldown_ms < I2C_FAULT_BACKOFF_MAX_MS) fault_cooldown_ms *= 2;
    } else {
        zero_chip_recovery_attempts++;
        ESP_LOGW(TAG, "0 ADS chips detected (RTC_Alive=1, attempt %d/%d). Triggering rate-limited I2C recovery...",
                 zero_chip_recovery_attempts, MAX_ZERO_CHIP_RECOVERIES);
    }

    recover_i2c_bus();
    vTaskDelay(pdMS_TO_TICKS(150));
    if (perform_i2c_probe_pass(&rtc_ok) > 0) zero_chip_recovery_attempts = 0;
}

// ==========================================
// SAFE EJECT SLIDE SWITCH
// ==========================================
static void apply_safe_eject(bool active) {
    if (active) {
        // Wait for any in-flight transaction to finish before dropping the gate.
        // Gating mid-transaction would leave a slave holding SDA low.
        // 2 s: on a faulted bus one probe pass can hold the mutex well over 500 ms
        // (each failing transfer may run to I2C_XFER_TIMEOUT_MS).
        bool got_bus = (xSemaphoreTake(i2c_mutex, pdMS_TO_TICKS(2000)) == pdTRUE);
        safe_eject_active = true;
        gpio_set_level(TCA4307_EN_GPIO, 0);    // fault LED now slow-blinks (network_led_task)
        if (got_bus) xSemaphoreGive(i2c_mutex);
        ESP_LOGW(TAG, ">>> SAFE EJECT ACTIVATED: bus quiesced, gate locked (GPIO %d)%s <<<",
                 TCA4307_EN_GPIO, got_bus ? "" : " [bus was busy >2 s]");
    } else {
        // Boards may have been swapped during the eject: forget every ADS handle,
        // so a chip counts as present only once it answers a fresh probe. Without
        // this, a removed board kept its handle for 3 more scans and its read
        // errors escalated to a full bus teardown.
        if (xSemaphoreTake(i2c_mutex, pdMS_TO_TICKS(2000)) == pdTRUE) {
            for (int i = 0; i < 4; i++) {
                if (ads_handles[i] != NULL) { i2c_master_bus_rm_device(ads_handles[i]); ads_handles[i] = NULL; }
                probe_misses[i] = PROBE_MISS_LIMIT;
            }
            xSemaphoreGive(i2c_mutex);
        }
        // Raise the gate and let it settle BEFORE clearing the flag, so no task
        // touches the bus during the gate's pre-charge.
        gpio_set_level(TCA4307_EN_GPIO, 1);
        vTaskDelay(pdMS_TO_TICKS(150));
        safe_eject_active = false;
        gpio_set_level(SYSTEM_LED_FAULT, 0);   // after the flag, so the blink can't leave it on; rescan sets the real state
        ESP_LOGI(TAG, ">>> SAFE EJECT DEACTIVATED: gate open, rescanning <<<");
        xEventGroupSetBits(s_hardware_event_group, I2C_RESCAN_REQUIRED_BIT);
    }
}

void eject_button_task(void *pvParameter) {
    gpio_config_t btn_cfg = {
        .pin_bit_mask = (1ULL << EJECT_BUTTON_GPIO),
        .mode = GPIO_MODE_INPUT,
        .pull_up_en = GPIO_PULLUP_DISABLE,     // GPIO 34 has no internal pulls
        .pull_down_en = GPIO_PULLDOWN_DISABLE,
        .intr_type = GPIO_INTR_DISABLE
    };
    gpio_config(&btn_cfg);

    // Honour the switch position at boot (level 0 = switch to GND = eject).
    int last_level = gpio_get_level(EJECT_BUTTON_GPIO);
    if (last_level == 0) apply_safe_eject(true);

    // A change is accepted only after the pin has read the new level on every
    // 10 ms sample for EJECT_DEBOUNCE_MS. A real flip of the switch easily
    // passes; the ~100-250 ms glitches seen on GPIO 34 do not.
    uint32_t stable_ms = 0;
    while (1) {
        int level = gpio_get_level(EJECT_BUTTON_GPIO);
        if (level != last_level) {
            stable_ms += 10;
            if (stable_ms >= EJECT_DEBOUNCE_MS) {
                apply_safe_eject(level == 0);
                last_level = level;
                stable_ms = 0;
            }
        } else {
            stable_ms = 0;   // glitch ended before the window: ignore it
        }
        vTaskDelay(pdMS_TO_TICKS(10));
    }
}

// ==========================================
// ADS1115 POLLING TASK (ATOMIC)
// ==========================================
static uint8_t evaluate_port_status(int16_t raw_value) {
    if (raw_value == -9999) return 3;
    if (raw_value >= 32760 || raw_value <= -32760) return 2;
    if (raw_value >= -5 && raw_value <= 5) return 1;
    return 0;
}

void ads_reader_task(void *pvParameter) {
    // Counted per chip: with one counter for the whole bus, a single flaky or
    // unplugged board tore down the bus (and every other board) within one sweep.
    int  chip_errors[4] = {0};
    bool chip_warned[4] = {false};   // one RTC check + log per failure run

    while (1) {
        if (safe_eject_active) {
            memset(chip_errors, 0, sizeof(chip_errors));
            memset(chip_warned, 0, sizeof(chip_warned));
            vTaskDelay(pdMS_TO_TICKS(500));
            continue;
        }

        for (int i = 0; i < 4; i++) {
            if (ads_handles[i] == NULL || !global_node_data[i].is_online) continue;

            for (int channel = 0; channel < 4; channel++) {
                if (!port_active[i][channel]) continue;
                if (safe_eject_active) break;              // switch flipped mid-sweep

                uint8_t config_data[3] = { REG_POINTER_CONFIG, ADS_MUX_MSB[channel], ADS_CONFIG_LSB };
                uint8_t cfg_ptr  = REG_POINTER_CONFIG;
                uint8_t conv_ptr = REG_POINTER_CONVERT;
                uint8_t buf[2];
                int16_t final_val = -9999;

                // ATOMIC: hold i2c_mutex across write -> poll-until-done -> read,
                // so no other task can interleave a transaction mid-conversion.
                if (xSemaphoreTake(i2c_mutex, pdMS_TO_TICKS(100)) != pdTRUE) {
                    continue;   // bus busy: keep the last good value, don't write a false -9999
                }

                // Re-check inside the mutex: a probe pass or recovery may have
                // removed this handle while we waited. Without this the transmit
                // below is a use-after-free.
                if (ads_handles[i] == NULL) {
                    xSemaphoreGive(i2c_mutex);
                    break;
                }

                if (i2c_master_transmit(ads_handles[i], config_data, sizeof(config_data), I2C_XFER_TIMEOUT_MS) == ESP_OK) {
                    // Poll the OS bit (config bit 15: 0 = converting, 1 = done)
                    // instead of guessing a delay.
                    bool conv_done = false;
                    bool bus_error = false;

                    for (int tries = 0; tries < ADS_CONV_POLL_MAX_TRIES && !conv_done; tries++) {
                        vTaskDelay(pdMS_TO_TICKS(1));
                        if (i2c_master_transmit_receive(ads_handles[i], &cfg_ptr, 1, buf, sizeof(buf), I2C_XFER_TIMEOUT_MS) == ESP_OK) {
                            conv_done = ((buf[0] & 0x80) != 0);
                        } else {
                            bus_error = true;
                            break;
                        }
                    }

                    if (conv_done &&
                        i2c_master_transmit_receive(ads_handles[i], &conv_ptr, 1, buf, sizeof(buf), I2C_XFER_TIMEOUT_MS) == ESP_OK) {
                        final_val = (int16_t)((buf[0] << 8) | buf[1]);
                        chip_errors[i] = 0;
                        chip_warned[i] = false;
                    } else {
                        chip_errors[i]++;
                        if (!conv_done && !bus_error) {
                            ESP_LOGW(TAG, "ADS 0x%02X ch%d: conversion did not complete in %d ms",
                                     global_node_data[i].address, channel, ADS_CONV_POLL_MAX_TRIES);
                        }
                    }
                } else {
                    chip_errors[i]++;
                }

                xSemaphoreGive(i2c_mutex);

                if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
                    global_node_data[i].port_values[channel] = final_val;
                    xSemaphoreGive(data_mutex);
                }

                // Block briefly so lower-priority tasks (discovery scan) can
                // acquire the bus between conversions instead of timing out.
                vTaskDelay(pdMS_TO_TICKS(1));
            }
        }

        // A single NACK localises to a -9999 on one channel. When one chip keeps
        // failing, the RTC is asked once: if it answers, the bus is fine and only
        // that board is bad, so nothing is torn down -- its channels report -9999
        // and the probe pass drops it if it stops answering altogether. Only a bus
        // the RTC can't use gets a full teardown.
        for (int i = 0; i < 4 && !safe_eject_active; i++) {
            if (chip_errors[i] < ADS_SUSTAINED_ERROR_LIMIT || chip_warned[i]) continue;

            bool bus_ok = false;
            if (xSemaphoreTake(i2c_mutex, pdMS_TO_TICKS(1000)) == pdTRUE) {
                // Two tries: one stray NACK must not tear down a working bus.
                bus_ok = (bus_handle != NULL &&
                          (probe_addr(DS3231_ADDR) == ESP_OK || probe_addr(DS3231_ADDR) == ESP_OK));
                xSemaphoreGive(i2c_mutex);
            }

            if (bus_ok) {
                ESP_LOGW(TAG, "ADS 0x%02X failing (%d reads in a row) but bus and RTC OK; check that board's power and wiring.",
                         possible_addresses[i], chip_errors[i]);
                chip_warned[i] = true;   // no repeat until a good read resets it
            } else {
                ESP_LOGW(TAG, "Sustained I2C fault (ADS 0x%02X: %d errors, RTC not answering). Triggering inline recovery...",
                         possible_addresses[i], chip_errors[i]);
                last_zero_chip_recovery_tick = xTaskGetTickCount();
                recover_i2c_bus();
                memset(chip_errors, 0, sizeof(chip_errors));
                memset(chip_warned, 0, sizeof(chip_warned));
                xEventGroupSetBits(s_hardware_event_group, I2C_RESCAN_REQUIRED_BIT);
                vTaskDelay(pdMS_TO_TICKS(5000));
                break;
            }
        }

        vTaskDelay(pdMS_TO_TICKS(50));
    }
}

// ==========================================
// DUPLICATE COMMAND SUPPRESSION
// ==========================================
// With disable_clean_session = true and QoS 1, the broker redelivers unacked
// commands from the persistent session on reconnect. Idempotent actions survive
// that; an 'actuate' would re-fire the pump each time. History is in RAM only,
// so a redelivery that arrives after a reboot will still execute.
// Touched only by command_worker_task, so no mutex is needed.
static char    recent_cids[CMD_DEDUP_HISTORY][MAX_CID_LEN] = {{0}};
static uint8_t recent_cid_idx = 0;

static bool cmd_already_seen(const char *cid) {
    for (int i = 0; i < CMD_DEDUP_HISTORY; i++) {
        if (recent_cids[i][0] != '\0' && strcmp(recent_cids[i], cid) == 0) return true;
    }
    return false;
}

static void cmd_remember(const char *cid) {
    snprintf(recent_cids[recent_cid_idx], MAX_CID_LEN, "%s", cid);
    recent_cid_idx = (uint8_t)((recent_cid_idx + 1) % CMD_DEDUP_HISTORY);
}

// ==========================================
// INCOMING COMMAND PARSER (runs in command_worker_task)
// ==========================================
static void process_incoming_command(const char *json_string, size_t length) {
    if (json_string == NULL || length == 0) return;

    cJSON *root = cJSON_ParseWithLength(json_string, length);
    if (!root) {
        ESP_LOGE(TAG, "Failed to parse incoming command payload as valid JSON.");
        return;
    }

    // 1. Strict CID validation & safe copy
    cJSON *cid_item = cJSON_GetObjectItem(root, "cid");
    if (!cJSON_IsString(cid_item) || cid_item->valuestring == NULL || strlen(cid_item->valuestring) == 0) {
        ESP_LOGW(TAG, "Command rejected: missing or invalid 'cid'");
        cJSON_Delete(root);
        return;
    }
    char cid_buf[MAX_CID_LEN];
    snprintf(cid_buf, sizeof(cid_buf), "%s", cid_item->valuestring);

    // 2. Strict action check
    cJSON *act_item = cJSON_GetObjectItem(root, "action");
    if (!cJSON_IsString(act_item) || act_item->valuestring == NULL) {
        ESP_LOGW(TAG, "Command rejected: missing or invalid 'action'");
        send_command_ack(cid_buf, "unknown", "failed", "Missing or invalid 'action' field");
        cJSON_Delete(root);
        return;
    }
    const char *action = act_item->valuestring;

    // 3. Tenant / node binding. A command naming a different tenant or node is
    //    never executed and never ACKed (an ACK would publish to OUR tenant's
    //    topic about another tenant's command). Fields absent from the payload
    //    are not required, but if present they must match.
    cJSON *tid_item = cJSON_GetObjectItem(root, "tid");
    cJSON *nid_item = cJSON_GetObjectItem(root, "nid");
    if ((cJSON_IsString(tid_item) && tid_item->valuestring && strcmp(tid_item->valuestring, tenant_id) != 0) ||
        (cJSON_IsString(nid_item) && nid_item->valuestring && strcmp(nid_item->valuestring, node_id) != 0)) {
        ESP_LOGE(TAG, "Command [%s] rejected: addressed to tid=%s nid=%s, this node is %s / %s.",
                 cid_buf,
                 cJSON_IsString(tid_item) ? tid_item->valuestring : "-",
                 cJSON_IsString(nid_item) ? nid_item->valuestring : "-",
                 tenant_id, node_id);
        cJSON_Delete(root);
        return;
    }

    // 4. Staleness. A persistent broker session can deliver commands that were
    //    queued hours ago. Only enforceable once the node's clock is valid; before
    //    the first SNTP sync the age cannot be judged, so the command is allowed.
    cJSON *ts_item = cJSON_GetObjectItem(root, "ts");
    time_t now = node_epoch();
    if (cJSON_IsNumber(ts_item) && now >= MIN_VALID_EPOCH) {
        double age = difftime(now, (time_t)ts_item->valuedouble);
        if (age > CMD_MAX_AGE_SEC) {
            ESP_LOGW(TAG, "Command [%s] action [%s] expired: issued %.0f s ago (limit %d s). Not executing.",
                     cid_buf, action, age, CMD_MAX_AGE_SEC);
            char details[80];
            snprintf(details, sizeof(details), "Command expired: issued %.0f s ago", age);
            send_command_ack(cid_buf, action, "failed", details);
            cJSON_Delete(root);
            return;
        }
    }

    // 5. Broker redelivery guard. The command DID execute on first receipt, so
    //    "completed" is accurate; the details field carries the distinction.
    if (cmd_already_seen(cid_buf)) {
        ESP_LOGW(TAG, "Duplicate command ID [%s] action [%s] -- broker redelivery, not re-executing.",
                 cid_buf, action);
        send_command_ack(cid_buf, action, "completed", "Command already executed; redelivery ignored");
        cJSON_Delete(root);
        return;
    }
    cmd_remember(cid_buf);

    // --- ACTION: BUS RECOVERY ---
    if (strcmp(action, "bus_recovery") == 0) {
        if (safe_eject_active) {
            send_command_ack(cid_buf, action, "failed",
                             "Safe eject is active; bus recovery refused while a pod may be mid-swap");
        } else {
            send_command_ack(cid_buf, action, "started", "Beginning I2C recovery cycle");
            recover_i2c_bus();
            xEventGroupSetBits(s_hardware_event_group, I2C_RESCAN_REQUIRED_BIT);
            send_command_ack(cid_buf, action, "completed", "I2C bus recovery complete");
        }
    }

    // --- ACTION: ACTUATE ---
    else if (strcmp(action, "actuate") == 0) {
        cJSON *port_item = cJSON_GetObjectItem(root, "port");
        cJSON *mode_item = cJSON_GetObjectItem(root, "mode");
        cJSON *dur_item  = cJSON_GetObjectItem(root, "dur");

        if (!cJSON_IsNumber(port_item) || !cJSON_IsString(mode_item) || mode_item->valuestring == NULL) {
            send_command_ack(cid_buf, action, "failed", "Invalid or missing 'port' or 'mode'");
            cJSON_Delete(root);
            return;
        }

        int raw_port = port_item->valueint;
        if (raw_port < 1 || raw_port > NUM_ACTUATORS) {
            send_command_ack(cid_buf, action, "failed", "Port out of bounds");
            cJSON_Delete(root);
            return;
        }
        int target_idx = raw_port - 1;
        const char *mode_str = mode_item->valuestring;

        // Duration: 0 = indefinite, capped at 24 h
        int duration_ms = 0;
        if (cJSON_IsNumber(dur_item)) {
            if (dur_item->valueint > 0 && dur_item->valueint <= 86400000) duration_ms = dur_item->valueint;
            else if (dur_item->valueint > 86400000) duration_ms = 86400000;
        }

        // 1. Validate and resolve the requested output BEFORE touching hardware
        bool is_binary = false;
        int  bin_state = 0;
        int  pwm_duty  = 0;

        if (strcmp(mode_str, "bin") == 0) {
            cJSON *state_item = cJSON_GetObjectItem(root, "state");
            if (!cJSON_IsNumber(state_item)) {
                send_command_ack(cid_buf, action, "failed", "Binary mode requires numeric 'state' (0 or 1)");
                cJSON_Delete(root);
                return;
            }
            is_binary = true;
            bin_state = (state_item->valueint > 0) ? 1 : 0;
        }
        else if (strcmp(mode_str, "pwm") == 0) {
            cJSON *state_item = cJSON_GetObjectItem(root, "state");
            cJSON *duty_item  = cJSON_GetObjectItem(root, "duty");
            if (cJSON_IsNumber(state_item) && state_item->valueint == 0) {
                pwm_duty = 0;
            } else if (cJSON_IsNumber(duty_item)) {
                pwm_duty = duty_item->valueint;
                if (pwm_duty < 0)   pwm_duty = 0;
                if (pwm_duty > 255) pwm_duty = 255;
            } else {
                send_command_ack(cid_buf, action, "failed", "PWM mode requires numeric 'duty' or state:0");
                cJSON_Delete(root);
                return;
            }
        }
        else {
            send_command_ack(cid_buf, action, "failed", "Unsupported actuation mode");
            cJSON_Delete(root);
            return;
        }

        bool actuator_active_state = is_binary ? (bin_state == 1) : (pwm_duty > 0);

        // 2. Cancel any pending auto-shutoff on this port BEFORE driving it, so
        //    an expiring old timer can never switch off the new output.
        cancel_auto_shutoff(target_idx);

        // 3. Drive the output
        ledc_channel_t chan = actuator_channels[target_idx];
        if (is_binary) {
            ledc_stop(ACTUATOR_LEDC_MODE, chan, bin_state);
            ESP_LOGI(TAG, "Binary: OUT%d (GPIO %d) -> %d [Duration: %d ms]",
                     target_idx + 1, actuator_gpios[target_idx], bin_state, duration_ms);
        } else {
            uint32_t duty = (pwm_duty >= 255) ? ACTUATOR_DUTY_FULL : (uint32_t)pwm_duty;
            ledc_set_duty(ACTUATOR_LEDC_MODE, chan, duty);
            ledc_update_duty(ACTUATOR_LEDC_MODE, chan);
            ESP_LOGI(TAG, "PWM: OUT%d (GPIO %d) -> Duty: %d/255 [Duration: %d ms]",
                     target_idx + 1, actuator_gpios[target_idx], pwm_duty, duration_ms);
        }

        // 4. Arm auto-shutoff and acknowledge
        if (actuator_active_state && duration_ms > 0) {
            if (arm_auto_shutoff(target_idx, duration_ms, cid_buf)) {
                send_command_ack(cid_buf, action, "started", "Actuator driven high, auto-shutoff armed");
            } else {
                actuator_force_off(target_idx);
                ESP_LOGE(TAG, "Could not arm auto-shutoff on OUT%d; forced off for safety.", target_idx + 1);
                send_command_ack(cid_buf, action, "failed",
                                 "Could not arm auto-shutoff; actuator forced off for safety");
            }
        } else if (actuator_active_state) {
            send_command_ack(cid_buf, action, "started", "Actuator driven high indefinitely");
        } else {
            send_command_ack(cid_buf, action, "stopped", "Actuator set to default idle state");
        }
    }

    // --- ACTION: SENSOR PORT CONFIGURATION ---
    else if (strcmp(action, "sensor_port_up") == 0 || strcmp(action, "sensor_port_down") == 0) {
        cJSON *chip_item = cJSON_GetObjectItem(root, "chip");
        cJSON *ch_item   = cJSON_GetObjectItem(root, "ch");

        if (cJSON_IsNumber(chip_item) && cJSON_IsNumber(ch_item)) {
            int chip_idx = chip_item->valueint;
            int ch_idx   = ch_item->valueint;

            if (chip_idx >= 0 && chip_idx < 4 && ch_idx >= 0 && ch_idx < 4) {
                bool set_active = (strcmp(action, "sensor_port_up") == 0);
                send_command_ack(cid_buf, action, "started", "Modifying software configuration states");

                if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
                    if (set_active && !port_active[chip_idx][ch_idx]) {
                        global_node_data[chip_idx].port_values[ch_idx] = ADS_NO_DATA;   // value from before it was disabled is stale
                    }
                    port_active[chip_idx][ch_idx] = set_active;
                    xSemaphoreGive(data_mutex);
                }
                ESP_LOGW(TAG, "Altered configuration: Chip Index [%d] Port [%d] -> %s",
                         chip_idx, ch_idx, set_active ? "ENABLED" : "DISABLED");
                xEventGroupSetBits(s_hardware_event_group, I2C_RESCAN_REQUIRED_BIT);
                send_command_ack(cid_buf, action, "completed", "Target port map dynamically adjusted");
            } else {
                send_command_ack(cid_buf, action, "failed", "Chip or port argument range out of bounds");
            }
        } else {
            send_command_ack(cid_buf, action, "failed", "Missing numeric 'chip' or 'ch' parameters");
        }
    }
    else {
        send_command_ack(cid_buf, action, "failed", "Unknown action");
    }

    cJSON_Delete(root);
}

// ==========================================
// COMMAND WORKER (executes outside the MQTT task)
// ==========================================
void command_worker_task(void *pvParameter) {
    static cmd_msg_t msg;   // ~1 KB; static keeps it off this task's stack
    while (1) {
        if (xQueueReceive(command_queue, &msg, portMAX_DELAY) == pdTRUE) {
            process_incoming_command(msg.payload, msg.len);
        }
    }
}

// ==========================================
// CONFIGURATION BROADCASTER
// ==========================================
static void publish_node_config(void) {
    if (!is_mqtt_connected || mqtt_client == NULL) return;

    nvs_handle_t h;
    uint8_t mode = 0;
    char saved_ssid[64] = "unconfigured";
    char saved_apn[64]  = "unconfigured";
    char saved_uri[128] = "unconfigured";

    if (nvs_open("senseable", NVS_READONLY, &h) == ESP_OK) {
        size_t len;
        nvs_get_u8(h, "hw_mode", &mode);
        len = sizeof(saved_ssid); nvs_get_str(h, "wifi_ssid", saved_ssid, &len);
        len = sizeof(saved_apn);  nvs_get_str(h, "cell_apn", saved_apn, &len);
        len = sizeof(saved_uri);  nvs_get_str(h, "sec_uri", saved_uri, &len);
        nvs_close(h);
    }

    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "t", "config");
    cJSON_AddNumberToObject(root, "v", 1);
    cJSON_AddStringToObject(root, "tid", tenant_id);
    cJSON_AddStringToObject(root, "nid", node_id);
    cJSON_AddStringToObject(root, "active_hw_mode", mode == 0 ? "WIFI" : "CELLULAR");
    cJSON_AddStringToObject(root, "wifi_ssid", saved_ssid);
    cJSON_AddStringToObject(root, "cell_apn", saved_apn);
    cJSON_AddStringToObject(root, "target_broker", saved_uri);
    cJSON_AddNumberToObject(root, "ts", (double)node_epoch());

    char *payload = cJSON_PrintUnformatted(root);
    if (payload != NULL) {
        // publish() is documented as safe from the MQTT event handler, which is
        // where this is called from (MQTT_EVENT_CONNECTED).
        esp_mqtt_client_publish(mqtt_client, topic_config, payload, 0, 1, 1);
        ESP_LOGI(TAG, "Hardware Configuration Synced to Cloud -> %s", payload);
        free(payload);
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
        is_mqtt_connected = true;
        cloud_disconnect_count = 0;
        xEventGroupSetBits(s_network_event_group, MQTT_CONNECTED_BIT);
        xEventGroupSetBits(s_hardware_event_group, I2C_RESCAN_REQUIRED_BIT);

        esp_mqtt_client_subscribe(mqtt_client, topic_cmd, 1);
        ESP_LOGI(TAG, "Subscribed to Command Topic: %s", topic_cmd);

        esp_mqtt_client_publish(mqtt_client, topic_status, "{\"t\":\"lwt\",\"status\":\"online\"}", 0, 1, 1);
        publish_node_config();
    }
    else if (event_id == MQTT_EVENT_DISCONNECTED) {
        ESP_LOGW(TAG, "MQTT Disconnected.");
        is_mqtt_connected = false;
        xEventGroupClearBits(s_network_event_group, MQTT_CONNECTED_BIT);

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
        // esp-mqtt splits payloads larger than its buffer across several
        // MQTT_EVENT_DATA events. Our commands are far smaller, so a fragment
        // means something is wrong upstream: reject rather than parse half a command.
        if (event->current_data_offset != 0 || event->data_len != event->total_data_len) {
            ESP_LOGW(TAG, "Dropping fragmented command payload (offset %d, %d of %d bytes)",
                     event->current_data_offset, event->data_len, event->total_data_len);
            return;
        }
        // Only ever act on this node's own command topic. A persistent broker
        // session can retain a subscription from a previous provisioning (a
        // different tenant ID) and keep delivering that topic's messages.
        if (event->topic_len != (int)strlen(topic_cmd) ||
            strncmp(event->topic, topic_cmd, event->topic_len) != 0) {
            ESP_LOGW(TAG, "Ignoring message on foreign topic [%.*s] (this node listens on %s). Stale broker session?",
                     event->topic_len, event->topic, topic_cmd);
            return;
        }

        if (event->data_len <= 0 || event->data_len >= MAX_CMD_PAYLOAD_LEN) {
            ESP_LOGE(TAG, "Payload size (%d bytes) outside allowed limits. Dropping packet.", event->data_len);
            return;
        }

        ESP_LOGI(TAG, "Command Packet Received on [%.*s]: %.*s",
                 event->topic_len, event->topic, event->data_len, event->data);

        cmd_staging.len = (size_t)event->data_len;
        memcpy(cmd_staging.payload, event->data, event->data_len);
        cmd_staging.payload[event->data_len] = '\0';

        // Hand off and return immediately. Executing here would run I2C recovery,
        // actuator changes and ACK publishes inside the MQTT task, blocking
        // keepalives and inbound processing while they run.
        if (command_queue == NULL || xQueueSend(command_queue, &cmd_staging, 0) != pdTRUE) {
            ESP_LOGE(TAG, "Command queue full or uninitialised; dropping command.");
        }
    }
}

static void configure_mqtt_client(void) {
    xSemaphoreTake(mqtt_cfg_mutex, portMAX_DELAY);

    esp_mqtt_client_config_t mqtt_cfg = {0};
    mqtt_cfg.network.timeout_ms = MQTT_NETWORK_TIMEOUT_MS;

    mqtt_cfg.credentials.client_id = node_id;

    // Normally a persistent session: the broker queues commands for this node
    // across momentary drops (stale and duplicate deliveries are handled in the
    // command worker). In the boot where a tenant change is detected, connect
    // with a CLEAN session instead, so the broker discards the old tenant's
    // subscription and any commands it queued -- those must never execute on a
    // node that now belongs to someone else. The clean session lasts for the
    // rest of this boot; the next boot is persistent again.
    mqtt_cfg.session.disable_clean_session = !broker_session_purge;
    if (broker_session_purge) {
        ESP_LOGW(TAG, "Tenant handover: connecting with a CLEAN session (broker discards stored state for %s).",
                 node_id);
    }

    mqtt_cfg.session.last_will.topic   = topic_status;
    mqtt_cfg.session.last_will.msg     = "{\"t\":\"lwt\",\"status\":\"offline\"}";
    mqtt_cfg.session.last_will.qos     = 1;
    mqtt_cfg.session.last_will.retain  = 1;
    mqtt_cfg.session.keepalive         = MQTT_KEEPALIVE_SEC;

    if (current_route_state == ROUTE_CLOUD) {
        mqtt_cfg.broker.address.uri = pri_broker_uri;
        if (strncmp(pri_broker_uri, "mqtts://", strlen("mqtts://")) == 0) {
            mqtt_cfg.broker.verification.certificate = mosqmq_root_ca;
            mqtt_cfg.credentials.username = MQTT_USERNAME;
            mqtt_cfg.credentials.authentication.password = MQTT_PASSWORD;
        }
    } else {
        mqtt_cfg.broker.address.uri = sec_broker_uri;
        if (strncmp(sec_broker_uri, "mqtts://", strlen("mqtts://")) == 0) {
            mqtt_cfg.broker.verification.certificate = mosqmq_root_ca;
            mqtt_cfg.credentials.username = LOCAL_MQTT_USERNAME;
            mqtt_cfg.credentials.authentication.password = LOCAL_MQTT_PASSWORD;
        }
    }
    if (mqtt_client == NULL) {
        esp_mqtt_client_handle_t c = esp_mqtt_client_init(&mqtt_cfg);
        esp_mqtt_client_register_event(c, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
        mqtt_client = c;
        esp_mqtt_client_start(c);
    } else {
        // Re-point the existing client instead of destroying it: other tasks hold
        // the handle (a destroy under them is a use-after-free), and its outbox
        // keeps QoS 1 rows that were queued but not yet acknowledged.
        esp_mqtt_client_stop(mqtt_client);
        is_mqtt_connected = false;   // stop() does not raise MQTT_EVENT_DISCONNECTED
        xEventGroupClearBits(s_network_event_group, MQTT_CONNECTED_BIT);
        esp_mqtt_set_config(mqtt_client, &mqtt_cfg);
        esp_mqtt_client_start(mqtt_client);
    }
    xSemaphoreGive(mqtt_cfg_mutex);
}

// ==========================================
// WAN CONNECTIVITY PROBE
// ==========================================
static bool is_internet_available(void) {
    struct addrinfo hints = { .ai_family = AF_INET, .ai_socktype = SOCK_STREAM };
    struct addrinfo *res;
    int err = getaddrinfo("google.com", "80", &hints, &res);
    if (err == 0) {
        int sock = socket(res->ai_family, res->ai_socktype, 0);
        if (sock >= 0) {
            struct timeval to = { .tv_sec = WIFI_WAN_PROBE_TIMEOUT_SEC, .tv_usec = 0 };
            setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &to, sizeof(to));
            setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &to, sizeof(to));
            int conn_err = connect(sock, res->ai_addr, res->ai_addrlen);
            close(sock); freeaddrinfo(res);
            if (conn_err == 0) return true;
        } else {
            freeaddrinfo(res);
        }
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
            current_route_state = ROUTE_LOCAL;
            configure_mqtt_client();
        }

        if (current_hw_mode == HW_MODE_WIFI && current_route_state == ROUTE_LOCAL) {
            if ((xTaskGetTickCount() - last_cloud_ping) > pdMS_TO_TICKS(WIFI_WAN_PROBE_INTERVAL_MS)) {
                last_cloud_ping = xTaskGetTickCount();
                ESP_LOGI(TAG, "Watchdog probing WAN connectivity...");
                if (is_internet_available()) {
                    ESP_LOGI(TAG, "Internet restored! Returning to Cloud Phase 1...");
                    current_route_state = ROUTE_CLOUD;
                    cloud_disconnect_count = 0;
                    configure_mqtt_client();
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
        esp_netif_dns_info_t dns_info = {0};
        dns_info.ip.type = ESP_IPADDR_TYPE_V4;
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

    int rssi = 99, ber = 99;
    tries = 0;
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
    dte_config.uart_config.port_num  = MODEM_UART_PORT;
    dte_config.uart_config.tx_io_num = MODEM_UART_TX_PIN;
    dte_config.uart_config.rx_io_num = MODEM_UART_RX_PIN;
    dte_config.uart_config.baud_rate = MODEM_BAUD_RATE;

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
            ESP_LOGW(TAG, "Retrying modem bring-up in %d s...", CELLULAR_BRINGUP_RETRY_MS / 1000);
            vTaskDelay(pdMS_TO_TICKS(CELLULAR_BRINGUP_RETRY_MS));
        }

        EventBits_t bits = xEventGroupWaitBits(s_network_event_group, PPP_CONNECTED_BIT, pdFALSE, pdTRUE,
                                               pdMS_TO_TICKS(CELLULAR_NET_WAIT_TIMEOUT_MS));
        if (!(bits & PPP_CONNECTED_BIT)) {
            ESP_LOGE(TAG, "PPP did not get an IP within %d s. Restarting modem sequence.",
                     CELLULAR_NET_WAIT_TIMEOUT_MS / 1000);
            continue;
        }

        if (mqtt_client == NULL) { current_route_state = ROUTE_CLOUD; configure_mqtt_client(); }

        int down_seconds = 0;
        int mqtt_offline_seconds = 0;

        while (1) {
            if (xEventGroupGetBits(s_network_event_group) & PPP_CONNECTED_BIT) down_seconds = 0;
            else down_seconds++;

            if (!is_mqtt_connected) mqtt_offline_seconds++;
            else mqtt_offline_seconds = 0;

            if (down_seconds >= CELLULAR_PPP_DROP_TIMEOUT_SEC ||
                mqtt_offline_seconds >= CELLULAR_MQTT_OFFLINE_TIMEOUT_SEC) {
                ESP_LOGW(TAG, "Cellular link dead (PPP dropped or MQTT timeout). Forcing modem reset.");
                esp_modem_set_mode(modem_dce, ESP_MODEM_MODE_COMMAND);
                xEventGroupClearBits(s_network_event_group, PPP_CONNECTED_BIT);
                break;
            }
            vTaskDelay(pdMS_TO_TICKS(1000));
        }
    }
}

// ==========================================
// TELEMETRY PUBLISHER
// ==========================================
// Separate from the builder because esp_mqtt_client_enqueue() waits for the
// MQTT client's internal lock, and the MQTT task holds that lock for the whole
// network timeout while a write to a dead link stalls. In the field that froze
// the builder for 112 s: those rows were neither sent nor spooled.
void telemetry_publisher_task(void *pvParameter) {
    char *s;
    while (1) {
        if (xQueueReceive(tlm_out_queue, &s, portMAX_DELAY) != pdTRUE) continue;
        if (is_mqtt_connected && mqtt_client != NULL &&
            esp_mqtt_client_enqueue(mqtt_client, topic_tlm, s, 0, 1, 0, true) >= 0) {
            ESP_LOGI(TAG, "Live Payload Dispatched -> %s", s);
        } else {
            spool_append_line(s);   // only queued with a valid clock, so no re-time fields needed
        }
        free(s);
    }
}

// ==========================================
// TELEMETRY BUILDER
// ==========================================
void telemetry_builder_task(void *pvParameter) {
    uint32_t cycle = 0;

    while (1) {
        cJSON *root = cJSON_CreateObject();
        cJSON_AddStringToObject(root, "t", "tlm");
        cJSON_AddNumberToObject(root, "v", 1);
        cJSON_AddStringToObject(root, "tid", tenant_id);
        cJSON_AddStringToObject(root, "nid", node_id);
        cJSON_AddStringToObject(root, "net", (current_hw_mode == HW_MODE_CELLULAR) ? "cell" : "wifi");
        cJSON_AddNumberToObject(root, "ts", (double)node_epoch());

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
                    if (port_active[i][channel] && global_node_data[i].port_values[channel] != ADS_NO_DATA) {
                        int16_t raw_reading = global_node_data[i].port_values[channel];
                        int port_data[3] = { channel, raw_reading, evaluate_port_status(raw_reading) };
                        cJSON_AddItemToArray(ports_array, cJSON_CreateIntArray(port_data, 3));
                    }
                }
                cJSON_AddItemToArray(adc_array, chip_obj);
            }
            xSemaphoreGive(data_mutex);
        }

        // Until the clock is valid, spool instead of publishing live: a live row
        // stamped with time-since-boot is dropped by the backend, while a spooled
        // row records its uptime and is re-timed by the replay once SNTP syncs.
        if (is_mqtt_connected && mqtt_client != NULL && node_epoch() >= MIN_VALID_EPOCH) {
            // Hand the row to telemetry_publisher_task and never wait for it: if
            // the publisher is stuck behind a stalled MQTT write, the row goes to
            // flash instead of being lost.
            char *payload_string = cJSON_PrintUnformatted(root);
            if (payload_string != NULL && xQueueSend(tlm_out_queue, &payload_string, 0) != pdTRUE) {
                free(payload_string);
                ESP_LOGW(TAG, "MQTT link stalled; spooling telemetry.");
                spool_telemetry_root(root);
            }
        } else {
            // Spool in BOTH modes: Wi-Fi can lose the cloud AND the edge broker at once.
            spool_telemetry_root(root);
        }
        cJSON_Delete(root);

        if (++cycle % HEALTH_LOG_EVERY_N_CYCLES == 0) {
            ESP_LOGI(TAG, "Health: heap=%" PRIu32 " min_heap=%" PRIu32 " outbox=%d route=%s mqtt=%d chips=%d",
                     esp_get_free_heap_size(), esp_get_minimum_free_heap_size(),
                     mqtt_client ? esp_mqtt_client_get_outbox_size(mqtt_client) : -1,
                     current_route_state == ROUTE_CLOUD ? "CLOUD" : "LOCAL",
                     is_mqtt_connected, num_ads_found);
        }

        vTaskDelay(pdMS_TO_TICKS(TELEMETRY_INTERVAL_MS));
    }
}

// ==========================================
// DISCOVERY BUILDER
// ==========================================
// Reports signal VALIDITY, not physical presence. There is no pull-down on the
// ADC inputs (one would load these high-impedance sensors and corrupt their
// readings), so an unconnected input is electrically undefined. With the PGA at
// +/-6.144 V and a 5 V supply, an input railed to VDD reads ~26,670 counts --
// identical to a sensor at full scale -- so level alone cannot tell them apart.
// KNOWN LIMITATION: an unconnected input reports CONNECTED; only I2C
// communication failure reports DISCONNECTED.
static bool is_sensor_attached(int16_t raw_value) {
    return (raw_value != -9999);
}

// Require PORT_DEBOUNCE_SAMPLES consecutive agreeing scans before a state change
// is believed, so one glitched read doesn't fire a retained discovery packet.
// Touched only by discovery_builder_task, so no mutex is needed.
static uint8_t port_stable_count[4][4]   = {{0}};
static bool    port_last_raw_state[4][4] = {{false}};
static bool    port_believed_state[4][4] = {{false}};
static bool    port_known[4][4]          = {{false}};   // false until the first sample after (re)appearing

static bool debounce_port_state(int chip, int ch, bool raw_now) {
    // First sample after a chip appears (or a port is re-enabled) is taken as-is.
    // Debouncing from an assumed "false" made every new chip report DISCONNECTED
    // for ~9 s, and the backend reacted by sending sensor_port_down.
    if (!port_known[chip][ch]) {
        port_known[chip][ch]          = true;
        port_last_raw_state[chip][ch] = raw_now;
        port_believed_state[chip][ch] = raw_now;
        port_stable_count[chip][ch]   = PORT_DEBOUNCE_SAMPLES;
        return raw_now;
    }
    if (raw_now == port_last_raw_state[chip][ch]) {
        if (port_stable_count[chip][ch] < PORT_DEBOUNCE_SAMPLES) port_stable_count[chip][ch]++;
    } else {
        port_last_raw_state[chip][ch] = raw_now;
        port_stable_count[chip][ch]   = 1;
    }
    if (port_stable_count[chip][ch] >= PORT_DEBOUNCE_SAMPLES) port_believed_state[chip][ch] = raw_now;
    return port_believed_state[chip][ch];
}

void discovery_builder_task(void *pvParameter) {
    uint32_t last_detailed_topology = 0xFFFFFFFF;
    TickType_t last_disco_publish = 0;

    while (1) {
        EventBits_t bits = xEventGroupWaitBits(s_hardware_event_group, I2C_RESCAN_REQUIRED_BIT,
                                               pdTRUE, pdFALSE, pdMS_TO_TICKS(I2C_FAST_SCAN_INTERVAL_MS));
        if (bits & I2C_RESCAN_REQUIRED_BIT) vTaskDelay(pdMS_TO_TICKS(200));

        scan_i2c_bus();

        // Give ads_reader_task time to read newly discovered chips
        if (num_ads_found > 0) vTaskDelay(pdMS_TO_TICKS(100));

        if (!is_mqtt_connected || mqtt_client == NULL) continue;

        uint32_t current_detailed_topology = 0;
        bool local_port_connected_map[4][4] = { {false} };

        if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
            int bit_shift_index = 0;
            for (int i = 0; i < 4; i++) {
                if (global_node_data[i].is_online) current_detailed_topology |= (1UL << bit_shift_index);
                bit_shift_index++;
                for (int channel = 0; channel < 4; channel++) {
                    if (global_node_data[i].is_online && port_active[i][channel]) {
                        bool raw_attached = is_sensor_attached(global_node_data[i].port_values[channel]);
                        bool attached     = debounce_port_state(i, channel, raw_attached);
                        local_port_connected_map[i][channel] = attached;
                        if (attached) current_detailed_topology |= (1UL << bit_shift_index);
                    } else {
                        port_known[i][channel] = false;
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
        cJSON_AddStringToObject(root, "net", (current_hw_mode == HW_MODE_CELLULAR) ? "cell" : "wifi");
        cJSON_AddNumberToObject(root, "ts", (double)node_epoch());
        cJSON_AddNumberToObject(root, "tlm_interval_ms", TELEMETRY_INTERVAL_MS);

        cJSON *bus_array = cJSON_AddArrayToObject(root, "buses");
        if (xSemaphoreTake(data_mutex, portMAX_DELAY) == pdTRUE) {
            cJSON_AddNumberToObject(root, "detected_chips", num_ads_found);
            for (int i = 0; i < 4; i++) {
                if (!global_node_data[i].is_online) continue;
                cJSON *chip_obj = cJSON_CreateObject();
                char hex_addr[5];
                sprintf(hex_addr, "0x%02X", global_node_data[i].address);
                cJSON_AddStringToObject(chip_obj, "a", hex_addr);

                cJSON *ports_obj = cJSON_AddObjectToObject(chip_obj, "ports");
                for (int channel = 0; channel < 4; channel++) {
                    char port_key[6];
                    sprintf(port_key, "p%d", channel);
                    if (port_active[i][channel]) {
                        cJSON_AddStringToObject(ports_obj, port_key,
                                                local_port_connected_map[i][channel] ? "CONNECTED" : "DISCONNECTED");
                    } else {
                        cJSON_AddStringToObject(ports_obj, port_key, "DISABLED");
                    }
                }
                cJSON_AddItemToArray(bus_array, chip_obj);
            }
            xSemaphoreGive(data_mutex);
        }

        char *payload_string = cJSON_PrintUnformatted(root);
        if (mqtt_client != NULL && payload_string != NULL) {
            if (esp_mqtt_client_enqueue(mqtt_client, topic_disco, payload_string, 0, 1, 1, true) < 0) {
                ESP_LOGW(TAG, "Discovery enqueue failed.");
            } else {
                ESP_LOGW(TAG, "Topology Change Caught! New Discovery Packet Dispatched: %s", payload_string);
            }
        }
        free(payload_string);
        cJSON_Delete(root);
    }
}

// ==========================================
// TENANT HANDOVER
// ==========================================
// Called only once the offline backlog is fully delivered. Follows the order
// agreed with the backend, on the PREVIOUS tenant's topics:
//   1. backlog sent (already done by the caller)
//   2. retained {"t":"lwt","status":"offline"}  -> A's dashboard shows Offline at once
//   3. empty retained disco                     -> clears the stale retained copy
//   4. empty retained config                    -> stops stale settings replaying into A
// Then records the new tenant in NVS so later boots use a normal session.
// Returns false (and is retried on the next replay cycle) if anything fails.
static bool complete_tenant_handover(void) {
    if (previous_tenant_id[0] != '\0') {
        char old_status[128], old_disco[128], old_config[128];
        snprintf(old_status, sizeof(old_status), "%s/%s/%s/status", MQTT_TOPIC_ROOT, previous_tenant_id, node_id);
        snprintf(old_disco,  sizeof(old_disco),  "%s/%s/%s/disco",  MQTT_TOPIC_ROOT, previous_tenant_id, node_id);
        snprintf(old_config, sizeof(old_config), "%s/%s/%s/config", MQTT_TOPIC_ROOT, previous_tenant_id, node_id);

        if (esp_mqtt_client_publish(mqtt_client, old_status, "{\"t\":\"lwt\",\"status\":\"offline\"}", 0, 1, 1) < 0 ||
            esp_mqtt_client_publish(mqtt_client, old_disco,  "", 0, 1, 1) < 0 ||
            esp_mqtt_client_publish(mqtt_client, old_config, "", 0, 1, 1) < 0) {
            ESP_LOGW(TAG, "Tenant handover: could not close out %s yet; will retry.", previous_tenant_id);
            return false;
        }
        ESP_LOGW(TAG, "Tenant handover: %s closed out (status offline, retained disco and config cleared).",
                 previous_tenant_id);
    }

    nvs_handle_t h;
    esp_err_t err = nvs_open("senseable", NVS_READWRITE, &h);
    if (err == ESP_OK) {
        err = nvs_set_str(h, "last_tid", tenant_id);
        if (err == ESP_OK) err = nvs_commit(h);
        nvs_close(h);
    }
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "Tenant handover: could not record %s in NVS (%s); will retry.",
                 tenant_id, esp_err_to_name(err));
        return false;
    }

    handover_pending = false;
    ESP_LOGI(TAG, "Tenant handover complete. Node now belongs to %s.", tenant_id);
    return true;
}

// ==========================================
// OFFLINE BACKLOG REPLAY (both modes)
// ==========================================
// The live spool is renamed before replay so rows appended during the replay
// are never deleted unread. A replay interrupted by a disconnect is resumed
// (from the start of that file) on the next connection rather than discarded;
// a few rows may therefore be sent twice -- every replayed row carries "r":1.
//
// Each row is published on the topic of the tenant recorded INSIDE that row, so
// readings taken while the node belonged to tenant A go back to A even after it
// has been moved to B, and never appear on B's topics. (The backend also routes
// by the payload's tid, so this keeps the separation at the broker as well.)
static bool spool_has_data(void) {
    struct stat st;
    return (stat(SPOOL_REPLAY_PATH, &st) == 0 && st.st_size > 0) ||
           (stat(SPOOL_FILE_PATH, &st) == 0 && st.st_size > 0);
}

void backlog_replay_task(void *pvParameter) {
    static char line[1024];
    char row_topic[128];

    while (1) {
        xEventGroupWaitBits(s_network_event_group, MQTT_CONNECTED_BIT, pdFALSE, pdTRUE, portMAX_DELAY);

        bool replay_complete = true;
        struct stat st;
        bool have_replay = (stat(SPOOL_REPLAY_PATH, &st) == 0 && st.st_size > 0);
        if (!have_replay) {
            xSemaphoreTake(spool_mutex, portMAX_DELAY);   // no append may be mid-write during the rename
            if (stat(SPOOL_FILE_PATH, &st) == 0 && st.st_size > 0) {
                remove(SPOOL_REPLAY_PATH);   // clear any zero-length leftover
                have_replay = (rename(SPOOL_FILE_PATH, SPOOL_REPLAY_PATH) == 0);
            }
            xSemaphoreGive(spool_mutex);
        }

        if (have_replay) {
            // Give SNTP a chance first: rows recorded before the clock was valid
            // can only be re-timed once it is, and the backend drops them otherwise.
            TickType_t wait_start = xTaskGetTickCount();
            while (node_epoch() < MIN_VALID_EPOCH && is_mqtt_connected &&
                   (xTaskGetTickCount() - wait_start) < pdMS_TO_TICKS(REPLAY_WAIT_FOR_TIME_MS)) {
                vTaskDelay(pdMS_TO_TICKS(1000));
            }
            if (node_epoch() < MIN_VALID_EPOCH) {
                ESP_LOGW(TAG, "Clock still not valid after %d s; replaying anyway (pre-sync rows will be undatable).",
                         REPLAY_WAIT_FOR_TIME_MS / 1000);
            }

            FILE *f = fopen(SPOOL_REPLAY_PATH, "r");
            if (f == NULL) {
                replay_complete = false;
            } else {
                // Count rows first so progress can be shown as "n / total".
                int total = 0;
                while (fgets(line, sizeof(line), f) != NULL) if (line[0] != '\n' && line[0] != '\0') total++;
                rewind(f);
                ESP_LOGW(TAG, "Link restored. Replaying %d spooled rows (~%d s)...", total, (total * 150) / 1000 + 1);
                int sent = 0, retimed = 0, undatable = 0, skipped = 0;

                while (fgets(line, sizeof(line), f) != NULL) {
                    line[strcspn(line, "\r\n")] = '\0';
                    if (line[0] == '\0') continue;
                    if (!is_mqtt_connected || mqtt_client == NULL) { replay_complete = false; break; }

                    cJSON *saved = cJSON_Parse(line);
                    if (saved == NULL) { skipped++; continue; }   // torn line from a power loss mid-write

                    // ---- Re-time rows recorded before the clock was valid
                    cJSON *up_item  = cJSON_GetObjectItem(saved, "_up");
                    cJSON *bid_item = cJSON_GetObjectItem(saved, "_b");
                    if (cJSON_IsNumber(up_item)) {
                        time_t now = node_epoch();
                        if (now >= MIN_VALID_EPOCH && cJSON_IsNumber(bid_item) &&
                            (uint32_t)bid_item->valuedouble == boot_id) {
                            double age = uptime_sec() - up_item->valuedouble;
                            if (age < 0) age = 0;
                            cJSON_ReplaceItemInObject(saved, "ts", cJSON_CreateNumber((double)now - age));
                            retimed++;
                        } else {
                            undatable++;   // recorded in an earlier boot: cannot be recovered
                        }
                        cJSON_DeleteItemFromObject(saved, "_up");
                        cJSON_DeleteItemFromObject(saved, "_b");
                    }

                    // ---- Route by the tenant recorded in the row
                    cJSON *tid_item = cJSON_GetObjectItem(saved, "tid");
                    const char *row_tid = (cJSON_IsString(tid_item) && tid_item->valuestring)
                                          ? tid_item->valuestring : tenant_id;
                    if (!tenant_id_is_safe(row_tid)) {
                        ESP_LOGW(TAG, "Skipping spooled row with invalid tid.");
                        cJSON_Delete(saved);
                        skipped++;
                        continue;
                    }
                    snprintf(row_topic, sizeof(row_topic), "%s/%s/%s/tlm", MQTT_TOPIC_ROOT, row_tid, node_id);

                    cJSON_AddNumberToObject(saved, "r", 1);
                    char *replay_payload = cJSON_PrintUnformatted(saved);
                    cJSON_Delete(saved);
                    if (replay_payload == NULL) { skipped++; continue; }

                    // publish() (blocking) is deliberate here: this is a dedicated
                    // task, and pacing one-at-a-time keeps a large backlog out of
                    // the MQTT outbox's RAM.
                    int msg_id = esp_mqtt_client_publish(mqtt_client, row_topic, replay_payload, 0, 1, 0);
                    free(replay_payload);
                    if (msg_id < 0) { replay_complete = false; break; }
                    sent++;
                    if (sent % 25 == 0) ESP_LOGI(TAG, "Replay progress: %d / %d rows sent", sent, total);
                    vTaskDelay(pdMS_TO_TICKS(150));
                }
                fclose(f);

                if (replay_complete) {
                    remove(SPOOL_REPLAY_PATH);
                    ESP_LOGI(TAG, "Backlog replay complete: %d rows sent (%d re-timed, %d undatable), %d skipped. Spool cleared.",
                             sent, retimed, undatable, skipped);
                } else {
                    // The whole file is replayed again on reconnect: rows already sent
                    // over a link that was dying may never have arrived, so they are
                    // re-sent (marked "r":1). The backend should de-duplicate on nid+ts.
                    ESP_LOGW(TAG, "Backlog replay interrupted after %d rows; the full backlog is re-sent on reconnect.", sent);
                }
            }
        }

        // Close out the previous tenant only once its backlog has been delivered.
        if (handover_pending && replay_complete && is_mqtt_connected && mqtt_client != NULL && !spool_has_data()) {
            complete_tenant_handover();
        }

        vTaskDelay(pdMS_TO_TICKS(5000));
    }
}

// ==========================================
// WI-FI STATION SETUP
// ==========================================
static void wifi_event_handler(void *arg, esp_event_base_t event_base, int32_t event_id, void *event_data) {
    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
    } else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
        ESP_LOGW(TAG, "Wi-Fi disconnected. Retrying...");
        xEventGroupClearBits(s_network_event_group, MQTT_CONNECTED_BIT);
        esp_wifi_connect();
    } else if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *event = (ip_event_got_ip_t *)event_data;
        ESP_LOGI(TAG, "Wi-Fi got IP: " IPSTR, IP2STR(&event->ip_info.ip));
        if (current_route_state == ROUTE_LOCAL && is_internet_available()) {
            ESP_LOGI(TAG, "Internet verified on new IP lease. Switching to Cloud Phase 1...");
            current_route_state = ROUTE_CLOUD;
            cloud_disconnect_count = 0;
            configure_mqtt_client();
        } else if (mqtt_client == NULL) {
            current_route_state = ROUTE_CLOUD;
            configure_mqtt_client();
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
    // Bounded copies: sta.ssid is 32 bytes but wifi_ssid is 64. A 32-character
    // SSID legally has no terminator, so copy by length rather than strcpy.
    memcpy(wifi_config.sta.ssid, wifi_ssid, strnlen(wifi_ssid, sizeof(wifi_config.sta.ssid)));
    memcpy(wifi_config.sta.password, wifi_pass, strnlen(wifi_pass, sizeof(wifi_config.sta.password)));

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

    // The i2c driver logs 2 warnings + 2 errors per failed transaction; at
    // 115200 baud that buries all THESIS_NODE output. Our own recovery
    // warnings carry the same information in one line.
    esp_log_level_set("i2c.master", ESP_LOG_NONE);
    esp_log_level_set("i2c.common", ESP_LOG_NONE);

    ESP_LOGI(TAG, "FreeRTOS tick rate: %d Hz", (int)configTICK_RATE_HZ);
    if (I2C_IRAM_SAFE_ENABLED) {
        ESP_LOGI(TAG, "I2C ISR IRAM-safe: enabled");
    } else {
        ESP_LOGW(TAG, "I2C ISR IRAM-safe: NOT enabled (flash writes may time out I2C; 500 ms timeouts still cover it)");
    }

    // All timestamps are UTC end to end: DS3231 registers, mktime/gmtime, payloads.
    setenv("TZ", "UTC0", 1);
    tzset();

    init_littlefs();
    init_dynamic_identity();
    init_system_leds();

    esp_netif_init();
    esp_event_loop_create_default();

    // Provisioning switch (GPIO 32, internal pull-up)
    gpio_config_t switch_cfg = {
        .pin_bit_mask = (1ULL << PROVISION_SWITCH_GPIO),
        .mode = GPIO_MODE_INPUT,
        .pull_up_en = GPIO_PULLUP_ENABLE,
        .pull_down_en = GPIO_PULLDOWN_DISABLE,
        .intr_type = GPIO_INTR_DISABLE
    };
    gpio_config(&switch_cfg);
    vTaskDelay(pdMS_TO_TICKS(10));

    // CAPTIVE PORTAL PROVISIONING MODE
    if (gpio_get_level(PROVISION_SWITCH_GPIO) == 0) {
        ESP_LOGW(TAG, "===============================================");
        ESP_LOGW(TAG, " CONFIG MODE TRIGGERED (SLIDE SWITCH GND)      ");
        ESP_LOGW(TAG, " Broadcasting SoftAP Captive Portal...         ");
        ESP_LOGW(TAG, "===============================================");
        wifi_init_softap();
        xTaskCreate(dns_server_task, "dns_task", 3072, NULL, 5, NULL);
        start_captive_web_server();
        while (1) vTaskDelay(pdMS_TO_TICKS(1000));
    }

    // NORMAL RUN MODE
    ESP_LOGI(TAG, "RUN MODE DETECTED. Loading NVS configuration...");
    nvs_handle_t h;
    if (nvs_open("senseable", NVS_READONLY, &h) == ESP_OK) {
        size_t len;
        len = sizeof(wifi_ssid);      nvs_get_str(h, "wifi_ssid", wifi_ssid, &len);
        len = sizeof(wifi_pass);      nvs_get_str(h, "wifi_pass", wifi_pass, &len);
        len = sizeof(cellular_apn);   nvs_get_str(h, "cell_apn", cellular_apn, &len);
        len = sizeof(sec_broker_uri); nvs_get_str(h, "sec_uri", sec_broker_uri, &len);
        len = sizeof(tenant_id);      nvs_get_str(h, "tenant_id", tenant_id, &len);
        uint8_t mode = 0;
        if (nvs_get_u8(h, "hw_mode", &mode) == ESP_OK) current_hw_mode = (HardwareConfig_t)mode;
        nvs_close(h);
    }

    // Unprovisioned-node kill switch
    if (strlen(tenant_id) == 0) {
        ESP_LOGE(TAG, "FATAL: Board is unprovisioned (missing Tenant ID).");
        ESP_LOGE(TAG, "Halting. Slide the provisioning switch to GND and use the captive portal.");
        while (1) {
            gpio_set_level(SYSTEM_LED_FAULT, 1); vTaskDelay(pdMS_TO_TICKS(100));
            gpio_set_level(SYSTEM_LED_FAULT, 0); vTaskDelay(pdMS_TO_TICKS(100));
        }
    }

    // ---- Tenant history: detect re-provisioning to a different tenant ----
    {
        char last_tid[32] = "";
        bool have_history = false;
        nvs_handle_t th;
        if (nvs_open("senseable", NVS_READONLY, &th) == ESP_OK) {
            size_t len = sizeof(last_tid);
            have_history = (nvs_get_str(th, "last_tid", last_tid, &len) == ESP_OK && last_tid[0] != '\0');
            nvs_close(th);
        }

        if (!have_history) {
            // First boot of this firmware (or freshly erased flash): the old
            // tenant is unknown, so nothing can be closed out -- but the broker
            // may still hold stale state for this client ID, so purge it once.
            broker_session_purge = true;
            handover_pending = true;
            ESP_LOGW(TAG, "No tenant history stored. Broker session will be purged once, then %s recorded.", tenant_id);
        } else if (strcmp(last_tid, tenant_id) != 0) {
            if (tenant_id_is_safe(last_tid)) {
                snprintf(previous_tenant_id, sizeof(previous_tenant_id), "%s", last_tid);
            }
            broker_session_purge = true;
            handover_pending = true;
            ESP_LOGW(TAG, "Tenant change detected: %s -> %s. Broker session will be purged; %s will be closed out after its backlog is sent.",
                     last_tid, tenant_id, last_tid);
        } else {
            ESP_LOGI(TAG, "Tenant unchanged (%s). Using persistent broker session.", tenant_id);
        }
    }
    boot_id = esp_random();

    build_mqtt_topics();

    i2c_mutex           = xSemaphoreCreateMutex();
    data_mutex          = xSemaphoreCreateMutex();
    task_tracking_mutex = xSemaphoreCreateMutex();
    s_network_event_group  = xEventGroupCreate();
    s_hardware_event_group = xEventGroupCreate();

    command_queue = xQueueCreate(CMD_QUEUE_DEPTH, sizeof(cmd_msg_t));
    tlm_out_queue = xQueueCreate(TLM_OUT_QUEUE_DEPTH, sizeof(char *));
    spool_mutex    = xSemaphoreCreateMutex();
    mqtt_cfg_mutex = xSemaphoreCreateMutex();
    if (command_queue == NULL) ESP_LOGE(TAG, "Failed to allocate command queue; C2 unavailable.");

    init_actuators();

    // A battery-backed DS3231 keeps its I2C state across an ESP32 reset. If the
    // reset landed mid-read, it is still holding SDA low: free it before the
    // first transaction, or the boot-time clock seed fails.
    i2c_bus_clear();

    if (i2c_master_init() == ESP_OK) {
        i2c_device_config_t ds_cfg = {
            .dev_addr_length = I2C_ADDR_BIT_LEN_7,
            .device_address = DS3231_ADDR,
            .scl_speed_hz = I2C_MASTER_FREQ_HZ
        };
        i2c_master_bus_add_device(bus_handle, &ds_cfg, &ds3231_handle);
        ESP_LOGI(TAG, "DS3231 RTC mapped to master bus.");
        seed_system_time_from_ds3231(true);
    } else {
        ESP_LOGE(TAG, "Failed to initialise the I2C master peripheral.");
    }

    // SNTP: polls pool.ntp.org whenever any netif (Wi-Fi or PPP) has a route.
    // The callback only sets a flag; time_sync_task writes the DS3231.
    esp_sntp_config_t sntp_cfg = ESP_NETIF_SNTP_DEFAULT_CONFIG("pool.ntp.org");
    sntp_cfg.sync_cb = on_sntp_sync;
    esp_netif_sntp_init(&sntp_cfg);

    // Let the A7670C power surge and the LLC settle before the first scan
    vTaskDelay(pdMS_TO_TICKS(2000));
    scan_i2c_bus();

    xTaskCreate(network_led_task,       "led_worker",  2048, NULL, 2, NULL);
    xTaskCreate(eject_button_task,      "eject_btn",   3072, NULL, 5, NULL);
    xTaskCreate(ads_reader_task,        "adc_worker",  3072, NULL, 5, NULL);
    xTaskCreate(telemetry_builder_task, "tlm_json",    4096, NULL, 5, NULL);
    xTaskCreate(telemetry_publisher_task, "tlm_pub",   3072, NULL, 5, NULL);
    xTaskCreate(discovery_builder_task, "disco_json",  4096, NULL, 4, NULL);
    xTaskCreate(backlog_replay_task,    "fifo_replay", 4096, NULL, 4, NULL);
    xTaskCreate(command_worker_task,    "cmd_worker",  4096, NULL, 5, NULL);
    xTaskCreate(time_sync_task,         "time_sync",   3072, NULL, 3, NULL);

    if (current_hw_mode == HW_MODE_CELLULAR) {
        ESP_LOGI(TAG, ">>> BOOTING CELLULAR MODEM (A7670C) <<<");
        xTaskCreate(cellular_task, "cellular", 6144, NULL, 5, NULL);
    } else {
        ESP_LOGI(TAG, ">>> BOOTING WI-FI STATION <<<");
        wifi_init_sta();
        xTaskCreate(cloud_watchdog_task, "cloud_wdog", 3072, NULL, 3, NULL);
    }
}