#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>

#include "esp_log.h"
#include "esp_wifi.h"
#include "esp_netif.h"
#include "esp_http_server.h"
#include "nvs_flash.h"
#include "nvs.h"
#include "lwip/sockets.h"

#include "captive_portal.h"

#define DNS_PORT 53
static httpd_handle_t portal_server = NULL;

static const char *HTML_CONFIG_PAGE =
"<!DOCTYPE html><html><head><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
"<style>"
"body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 20px; background: #f4f6f9; color: #333; }"
".card { background: white; border-radius: 12px; padding: 24px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); max-width: 400px; margin: auto; }"
"h2 { margin-top: 0; color: #1a365d; font-size: 20px; }"
"label { font-weight: 600; font-size: 13px; color: #4a5568; display: block; margin-top: 14px; margin-bottom: 4px; }"
"input { width: 100%; box-sizing: border-box; padding: 10px; border: 1px solid #cbd5e0; border-radius: 6px; font-size: 15px; }"
"button { width: 100%; margin-top: 22px; background: #2b6cb0; color: white; border: none; padding: 12px; border-radius: 6px; font-size: 16px; font-weight: bold; cursor: pointer; }"

"/* CSS-ONLY SEGMENTED TOGGLE (ZERO JS REQUIRED) */"
".toggle-wrap { display: flex; background: #edf2f7; border-radius: 8px; padding: 4px; margin-top: 8px; }"
".toggle-wrap label { flex: 1; text-align: center; padding: 8px 4px; margin: 0; cursor: pointer; border-radius: 6px; font-size: 13px; color: #4a5568; transition: all 0.2s; }"
"input[type=\"radio\"] { display: none; }"
"#tab-wifi:checked ~ .toggle-wrap label[for=\"tab-wifi\"], #tab-cell:checked ~ .toggle-wrap label[for=\"tab-cell\"] { background: #2b6cb0; color: white; font-weight: bold; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }"

"/* DYNAMIC FIELD SWITCHING */"
"#cell-fields { display: none; }"
"#wifi-fields { display: block; }"
"#tab-cell:checked ~ #cell-fields { display: block; }"
"#tab-cell:checked ~ #wifi-fields { display: none; }"
"#tab-wifi:checked ~ #cell-fields { display: none; }"
"#tab-wifi:checked ~ #wifi-fields { display: block; }"
"</style></head><body>"
"<div class=\"card\">"
"<h2>SENSEable Node Setup</h2>"
"<form action=\"/save\" method=\"POST\">"

"<input type=\"radio\" id=\"tab-wifi\" name=\"hw_mode\" value=\"0\" checked>"
"<input type=\"radio\" id=\"tab-cell\" name=\"hw_mode\" value=\"1\">"

"<label>Operating Mode</label>"
"<div class=\"toggle-wrap\">"
"<label for=\"tab-wifi\">Wi-Fi Mode</label>"
"<label for=\"tab-cell\">Cellular (A7670C)</label>"
"</div>"

"<div id=\"wifi-fields\">"
"<label>Wi-Fi SSID</label>"
"<input type=\"text\" name=\"ssid\" placeholder=\"Farm Wi-Fi Name\">"
"<label>Wi-Fi Password</label>"
"<input type=\"password\" name=\"pass\" placeholder=\"Wi-Fi Password\">"
"</div>"

"<div id=\"cell-fields\">"
"<label>Cellular APN</label>"
"<input type=\"text\" name=\"apn\" value=\"internet.globe.com.ph\" placeholder=\"e.g. internet.globe.com.ph\">"
"</div>"

"<label>Edge Broker URI (Mosquitto)</label>"
"<input type=\"text\" name=\"sec_uri\" value=\"mqtts://192.168.8.161:8883\" required>"
"<label>Tenant ID</label>"
"<input type=\"text\" name=\"tenant_id\" value=\"tenant123\" required>"

"<button type=\"submit\">Save & Apply</button>"
"</form></div></body></html>";

static const char *HTML_SAVED_PAGE =
"<!DOCTYPE html><html><head><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
"<style>body{font-family:sans-serif;text-align:center;padding:40px;background:#f0fff4;color:#22543d;}"
".card{background:white;padding:30px;border-radius:12px;box-shadow:0 4px 12px rgba(0,0,0,0.08);max-width:380px;margin:auto;}</style></head><body>"
"<div class=\"card\"><h2>Configuration Saved!</h2>"
"<p>Credentials securely stored in flash.</p>"
"<p><b>Next Step:</b> Flip the slide switch back to <b>Run Mode</b> and press the Reset button.</p>"
"</div></body></html>";

void dns_server_task(void *pvParameters) {
    uint8_t rx_buffer[128];
    uint8_t tx_buffer[128];
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_addr_len = sizeof(client_addr);

    int sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
    if (sock < 0) { vTaskDelete(NULL); return; }

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(DNS_PORT);
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        close(sock); vTaskDelete(NULL); return;
    }

    while (1) {
        int len = recvfrom(sock, rx_buffer, sizeof(rx_buffer), 0, (struct sockaddr *)&client_addr, &client_addr_len);
        if (len < 12) continue; 
        memcpy(tx_buffer, rx_buffer, len);
        tx_buffer[2] = 0x81; tx_buffer[3] = 0x80; tx_buffer[7] = 1;
        int idx = len;
        tx_buffer[idx++] = 0xC0; tx_buffer[idx++] = 0x0C;
        tx_buffer[idx++] = 0x00; tx_buffer[idx++] = 0x01; 
        tx_buffer[idx++] = 0x00; tx_buffer[idx++] = 0x01;
        tx_buffer[idx++] = 0x00; tx_buffer[idx++] = 0x00;
        tx_buffer[idx++] = 0x00; tx_buffer[idx++] = 0x3C; 
        tx_buffer[idx++] = 0x00; tx_buffer[idx++] = 0x04; 
        tx_buffer[idx++] = 192;  tx_buffer[idx++] = 168; tx_buffer[idx++] = 4; tx_buffer[idx++] = 1;
        sendto(sock, tx_buffer, idx, 0, (struct sockaddr *)&client_addr, client_addr_len);
    }
}

static void url_decode(char *dst, const char *src, size_t dst_size) {
    char a, b; 
    size_t d_idx = 0;
    while (*src && d_idx < dst_size - 1) {
        if (*src == '%' && ((a = src[1]) && (b = src[2])) && (isxdigit((int)a) && isxdigit((int)b))) {
            if (a >= 'a') { a -= 'a' - 'A'; }
            if (a >= 'A') { a -= ('A' - 10); } else { a -= '0'; }
            if (b >= 'a') { b -= 'a' - 'A'; }
            if (b >= 'A') { b -= ('A' - 10); } else { b -= '0'; }
            dst[d_idx++] = 16 * a + b; 
            src += 3;
        } else if (*src == '+') { 
            dst[d_idx++] = ' '; 
            src++;
        } else { 
            dst[d_idx++] = *src++; 
        }
    }
    dst[d_idx] = '\0';
}

static esp_err_t get_root_handler(httpd_req_t *req) {
    httpd_resp_set_type(req, "text/html");
    return httpd_resp_send(req, HTML_CONFIG_PAGE, HTTPD_RESP_USE_STRLEN);
}

static esp_err_t captive_redirect_handler(httpd_req_t *req) {
    httpd_resp_set_status(req, "302 Found");
    httpd_resp_set_hdr(req, "Location", "http://192.168.4.1/");
    return httpd_resp_send(req, NULL, 0);
}

static esp_err_t post_save_handler(httpd_req_t *req) {
    char buf[512];
    int ret, remaining = req->content_len;
    if (remaining >= sizeof(buf)) { httpd_resp_send_500(req); return ESP_FAIL; }
    ret = httpd_req_recv(req, buf, remaining);
    if (ret <= 0) return ESP_FAIL;
    buf[ret] = '\0';

    char raw_val[128];
    char clean_ssid[64] = {0}, clean_pass[64] = {0}, clean_sec_uri[128] = {0}, clean_tenant[32] = {0};
    char clean_apn[64] = "internet.globe.com.ph";
    uint8_t mode_val = 0;

    if (httpd_query_key_value(buf, "hw_mode", raw_val, sizeof(raw_val)) == ESP_OK) mode_val = (uint8_t)atoi(raw_val);
    if (httpd_query_key_value(buf, "ssid", raw_val, sizeof(raw_val)) == ESP_OK) url_decode(clean_ssid, raw_val, sizeof(clean_ssid));
    if (httpd_query_key_value(buf, "pass", raw_val, sizeof(raw_val)) == ESP_OK) url_decode(clean_pass, raw_val, sizeof(clean_pass));
    if (httpd_query_key_value(buf, "apn", raw_val, sizeof(raw_val)) == ESP_OK) url_decode(clean_apn, raw_val, sizeof(clean_apn));
    if (httpd_query_key_value(buf, "sec_uri", raw_val, sizeof(raw_val)) == ESP_OK) url_decode(clean_sec_uri, raw_val, sizeof(clean_sec_uri));
    if (httpd_query_key_value(buf, "tenant_id", raw_val, sizeof(raw_val)) == ESP_OK) url_decode(clean_tenant, raw_val, sizeof(clean_tenant));

    nvs_handle_t h;
    if (nvs_open("senseable", NVS_READWRITE, &h) == ESP_OK) {
        nvs_set_u8(h, "hw_mode", mode_val);
        if (strlen(clean_ssid) > 0) nvs_set_str(h, "wifi_ssid", clean_ssid);
        if (strlen(clean_pass) > 0) nvs_set_str(h, "wifi_pass", clean_pass);
        if (strlen(clean_apn) > 0) nvs_set_str(h, "cell_apn", clean_apn);
        nvs_set_str(h, "sec_uri", clean_sec_uri);
        nvs_set_str(h, "tenant_id", clean_tenant);
        nvs_commit(h); nvs_close(h);
        ESP_LOGI("PROVISION", "Credentials (including APN) committed to NVS.");
    }
    httpd_resp_set_type(req, "text/html");
    return httpd_resp_send(req, HTML_SAVED_PAGE, HTTPD_RESP_USE_STRLEN);
}

void start_captive_web_server(void) {
    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    config.max_uri_handlers = 8;
    if (httpd_start(&portal_server, &config) == ESP_OK) {
        httpd_uri_t root_uri = { .uri = "/", .method = HTTP_GET, .handler = get_root_handler };
        httpd_register_uri_handler(portal_server, &root_uri);
        httpd_uri_t save_uri = { .uri = "/save", .method = HTTP_POST, .handler = post_save_handler };
        httpd_register_uri_handler(portal_server, &save_uri);
        httpd_uri_t apple_uri = { .uri = "/hotspot-detect.html", .method = HTTP_GET, .handler = captive_redirect_handler };
        httpd_register_uri_handler(portal_server, &apple_uri);
        httpd_uri_t android_uri = { .uri = "/generate_204", .method = HTTP_GET, .handler = captive_redirect_handler };
        httpd_register_uri_handler(portal_server, &android_uri);
    }
}

void wifi_init_softap(void) {
    esp_netif_create_default_wifi_ap();
    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    char ap_ssid[64];
    sprintf(ap_ssid, "SENSEable-Setup-%s", node_id);
    wifi_config_t wifi_config = {
        .ap = {
            .ssid_len = strlen(ap_ssid), .channel = 1, .password = "", .max_connection = 4, .authmode = WIFI_AUTH_OPEN
        },
    };
    strcpy((char *)wifi_config.ap.ssid, ap_ssid);
    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_AP));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_AP, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());
    ESP_LOGI("PROVISION", "SoftAP active: SSID [%s] at 192.168.4.1", ap_ssid);
}