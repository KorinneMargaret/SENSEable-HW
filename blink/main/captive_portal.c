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

extern char node_id[32]; // Access the globally generated Node ID
extern const char ntp_server_default[]; // NTP_SERVER_LOCAL from credentials.h (main file)

// Stops phone keyboards from changing what is typed (auto-capitalising the first
// letter, "correcting" words). SSIDs, passwords and tenant IDs are case-sensitive,
// so an automatic edit made the node look for a network that doesn't exist.
// Capitals typed on purpose are kept.
#define NO_AUTO_FIX " autocapitalize=\"none\" autocorrect=\"off\" autocomplete=\"off\" spellcheck=\"false\""

static const char *HTML_CONFIG_PAGE =
"<!DOCTYPE html><html><head><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
"<style>"
"body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 20px; background: #f4f6f9; color: #333; }"
".card { background: white; border-radius: 12px; padding: 24px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); max-width: 400px; margin: auto; }"
"h2 { margin-top: 0; color: #1a365d; font-size: 20px; }"
"label { font-weight: 600; font-size: 13px; color: #4a5568; display: block; margin-top: 14px; margin-bottom: 4px; }"
"input { width: 100%; box-sizing: border-box; padding: 10px; border: 1px solid #cbd5e0; border-radius: 6px; font-size: 15px; }"
"button { width: 100%; margin-top: 22px; background: #2b6cb0; color: white; border: none; padding: 12px; border-radius: 6px; font-size: 16px; font-weight: bold; cursor: pointer; }"

"/* CSS-ONLY SEGMENTED TOGGLE */"
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

"<!-- DYNAMIC RADIO BUTTONS -->"
"<input type=\"radio\" id=\"tab-wifi\" name=\"hw_mode\" value=\"0\" %s>"
"<input type=\"radio\" id=\"tab-cell\" name=\"hw_mode\" value=\"1\" %s>"

"<label>Operating Mode</label>"
"<div class=\"toggle-wrap\">"
"<label for=\"tab-wifi\">Wi-Fi Mode</label>"
"<label for=\"tab-cell\">Cellular Mode</label>"
"</div>"

"<div id=\"wifi-fields\">"
"<label>Wi-Fi SSID</label>"
"<input type=\"text\" name=\"ssid\"" NO_AUTO_FIX " placeholder=\"Wi-Fi Name\" value=\"%s\">"
"<label>Wi-Fi Password <span style=\"font-weight:normal;color:#718096;\">(Leave blank to keep current)</span></label>"
"<input type=\"password\" name=\"pass\"" NO_AUTO_FIX " placeholder=\"New Wi-Fi Password\">"
"</div>"

"<div id=\"cell-fields\">"
"<label>Cellular APN</label>"
"<input type=\"text\" name=\"apn\"" NO_AUTO_FIX " placeholder=\"e.g. internet.globe.com.ph\" value=\"%s\">"
"</div>"

"<label>Edge Broker URI (Mosquitto)</label>"
"<input type=\"text\" name=\"sec_uri\"" NO_AUTO_FIX " placeholder=\"mqtts://...\" value=\"%s\" required>"
"<label>Time Server <span style=\"font-weight:normal;color:#718096;\">(laptop IP; blank = %s)</span></label>"
"<input type=\"text\" name=\"ntp_srv\"" NO_AUTO_FIX " maxlength=\"63\" pattern=\"^[A-Za-z0-9.\\-]*$\" title=\"IP address or host name\" placeholder=\"%s\" value=\"%s\">"
"<label>Telemetry Interval (seconds) <span style=\"font-weight:normal;color:#718096;\">(5-3600; each reading = average over this time)</span></label>"
"<input type=\"number\" name=\"tlm_s\" min=\"5\" max=\"3600\" step=\"1\" value=\"%lu\" required>"
"<label>Tenant ID</label>"
"<input type=\"text\" name=\"tenant_id\"" NO_AUTO_FIX " pattern=\"^[a-z0-9]{2,12}-[a-f0-9]{4}$\" title=\"Must be in format e.g. llda-add4\" placeholder=\"e.g. llda-add4\" value=\"%s\" required>"

"<button type=\"submit\">Save & Apply</button>"
"</form></div></body></html>";

// Shows what was ACTUALLY stored (read back from flash), so a typo or an
// autocorrected SSID can be spotted before switching to Run Mode.
static const char *HTML_SAVED_PAGE =
"<!DOCTYPE html><html><head><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
"<style>body{font-family:sans-serif;text-align:center;padding:40px;background:#f0fff4;color:#22543d;}"
".card{background:white;padding:30px;border-radius:12px;box-shadow:0 4px 12px rgba(0,0,0,0.08);max-width:380px;margin:auto;}"
"a{color:#2b6cb0;}</style></head><body>"
"<div class=\"card\"><h2>Configuration Saved!</h2>"
"<p style=\"margin:0;\">Check these settings before switching to Run Mode:</p>"
"<div style=\"background:#e2e8f0;padding:12px;border-radius:6px;margin:15px 0;text-align:left;font-family:monospace;color:#2d3748;font-size:14px;word-break:break-all;\">"
"<b>Node ID:</b> %s<br>"
"<b>Tenant ID:</b> %s<br>"
"<b>Mode:</b> %s<br>"
"<b>%s:</b> &quot;%s&quot;<br>"
"<b>Wi-Fi password:</b> %s<br>"
"<b>Edge broker:</b> %s<br>"
"<b>Time server:</b> %s<br>"
"<b>Telemetry interval:</b> %lu s"
"</div>"
"<p>Something wrong? <a href=\"/\">Go back and edit</a>, then save again.</p>"
"<p><b>Next Step:</b> Flip the slide switch back to <b>Run Mode</b> and press the Reset button.</p>"
"</div></body></html>";

// Captive-portal DNS: answers every A (IPv4) query with 192.168.4.1.
// The reply is built from the 12-byte header plus the question only. The old
// version copied the whole query (up to 128 bytes) into a 128-byte buffer and
// appended 16 more bytes, so a long query overflowed the buffer; it also put
// the answer after any extra records (EDNS) the phone sent, which made the
// reply malformed. Questions of other types (e.g. AAAA/IPv6) get a valid empty
// answer instead of an IPv4 address.
#define DNS_MAX_LEN     512                 // classic DNS-over-UDP maximum
#define DNS_HEADER_LEN  12
#define DNS_ANSWER_LEN  16

void dns_server_task(void *pvParameters) {
    // static: keeps ~1 KB off this task's 3 KB stack (only this task uses them)
    static uint8_t rx_buffer[DNS_MAX_LEN];
    static uint8_t tx_buffer[DNS_MAX_LEN + DNS_ANSWER_LEN];
    struct sockaddr_in server_addr, client_addr;

    int sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
    if (sock < 0) { vTaskDelete(NULL); return; }

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(DNS_PORT);
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        close(sock); vTaskDelete(NULL); return;
    }

    while (1) {
        socklen_t client_addr_len = sizeof(client_addr);
        int len = recvfrom(sock, rx_buffer, sizeof(rx_buffer), 0, (struct sockaddr *)&client_addr, &client_addr_len);
        if (len < DNS_HEADER_LEN) continue;

        // Only standard queries (QR = 0, opcode 0) with at least one question.
        if ((rx_buffer[2] & 0x80) || (rx_buffer[2] & 0x78)) continue;
        if (rx_buffer[4] == 0 && rx_buffer[5] == 0) continue;

        // Walk the first question's name (length-prefixed labels, ending in 0),
        // never reading past what was received.
        int pos = DNS_HEADER_LEN;
        bool name_ok = false;
        while (pos < len) {
            uint8_t label = rx_buffer[pos];
            if (label == 0) { pos++; name_ok = true; break; }
            if (label & 0xC0) break;                 // compression pointer: not valid in a query name
            pos += 1 + label;
        }
        if (!name_ok || pos + 4 > len) continue;     // also need QTYPE (2) + QCLASS (2)
        uint16_t qtype  = (uint16_t)((rx_buffer[pos] << 8) | rx_buffer[pos + 1]);
        uint16_t qclass = (uint16_t)((rx_buffer[pos + 2] << 8) | rx_buffer[pos + 3]);
        int question_end = pos + 4;

        // Header + question, copied as received (question_end <= len <= 512).
        memcpy(tx_buffer, rx_buffer, question_end);
        tx_buffer[2] = 0x81; tx_buffer[3] = 0x80;    // response, recursion desired/available, no error
        tx_buffer[4] = 0;    tx_buffer[5] = 1;       // 1 question
        tx_buffer[6] = 0;    tx_buffer[7] = 0;       // answers: set below
        tx_buffer[8] = 0;    tx_buffer[9] = 0;       // no authority records
        tx_buffer[10] = 0;   tx_buffer[11] = 0;      // no additional records (EDNS dropped)

        int idx = question_end;
        if (qtype == 1 && qclass == 1) {             // A record, Internet class
            tx_buffer[7] = 1;
            tx_buffer[idx++] = 0xC0; tx_buffer[idx++] = 0x0C;   // name: pointer to the question
            tx_buffer[idx++] = 0x00; tx_buffer[idx++] = 0x01;   // type A
            tx_buffer[idx++] = 0x00; tx_buffer[idx++] = 0x01;   // class IN
            tx_buffer[idx++] = 0x00; tx_buffer[idx++] = 0x00;
            tx_buffer[idx++] = 0x00; tx_buffer[idx++] = 0x3C;   // TTL 60 s
            tx_buffer[idx++] = 0x00; tx_buffer[idx++] = 0x04;   // 4-byte address
            tx_buffer[idx++] = 192;  tx_buffer[idx++] = 168; tx_buffer[idx++] = 4; tx_buffer[idx++] = 1;
        }
        sendto(sock, tx_buffer, idx, 0, (struct sockaddr *)&client_addr, client_addr_len);
    }
}

// Escapes text for safe use inside HTML (including attribute values), so a
// saved SSID or URI containing " < > & ' cannot break the setup page.
static void html_escape(char *dst, size_t dst_size, const char *src) {
    size_t d = 0;
    if (dst_size == 0) return;
    for (; src && *src; src++) {
        const char *rep = NULL;
        switch (*src) {
            case '&':  rep = "&amp;";  break;
            case '<':  rep = "&lt;";   break;
            case '>':  rep = "&gt;";   break;
            case '"':  rep = "&quot;"; break;
            case '\'': rep = "&#39;";  break;
            default: break;
        }
        size_t need = rep ? strlen(rep) : 1;
        if (d + need >= dst_size) break;             // never cut an entity in half
        if (rep) { memcpy(dst + d, rep, need); d += need; }
        else     { dst[d++] = *src; }
    }
    dst[d] = '\0';
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
    nvs_handle_t h;
    uint8_t mode = 0;
    char saved_ssid[64] = "";
    char saved_apn[64] = "internet.globe.com.ph"; // Fallback placeholder
    char saved_uri[128] = "";
    char saved_tenant[32] = "";
    char saved_ntp[64] = "";
    uint32_t saved_tlm_ms = 60000;   // default; remote setting wins, then the portal's

    // Fetch existing configurations
    if (nvs_open("senseable", NVS_READONLY, &h) == ESP_OK) {
        size_t len;
        nvs_get_u8(h, "hw_mode", &mode);
        
        len = sizeof(saved_ssid); nvs_get_str(h, "wifi_ssid", saved_ssid, &len);
        len = sizeof(saved_apn); nvs_get_str(h, "cell_apn", saved_apn, &len);
        len = sizeof(saved_uri); nvs_get_str(h, "sec_uri", saved_uri, &len);
        len = sizeof(saved_tenant); nvs_get_str(h, "tenant_id", saved_tenant, &len);
        len = sizeof(saved_ntp); nvs_get_str(h, "ntp_srv", saved_ntp, &len);
        if (nvs_get_u32(h, "tlm_ms", &saved_tlm_ms) != ESP_OK) nvs_get_u32(h, "tlm_ms_p", &saved_tlm_ms);
        
        nvs_close(h);
    }

    // Shown in the field: the saved server, else the built-in default
    const char *ntp_shown = saved_ntp[0] ? saved_ntp : ntp_server_default;
    const char *ntp_blank = ntp_server_default[0] ? ntp_server_default : "pool.ntp.org only";

    // Escape every saved value before it goes into the page (worst case each
    // character becomes 6, e.g. '"' -> "&quot;").
    char *esc = malloc(64 * 6 * 4 + 128 * 6 + 32 * 6);   // ssid, apn, ntp x2 | uri | tenant
    if (!esc) {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }
    char *e_ssid   = esc;                 // 384
    char *e_apn    = e_ssid  + 64 * 6;    // 384
    char *e_uri    = e_apn   + 64 * 6;    // 768
    char *e_blank  = e_uri   + 128 * 6;   // 384
    char *e_shown  = e_blank + 64 * 6;    // 384
    char *e_tenant = e_shown + 64 * 6;    // 192
    html_escape(e_ssid,   64 * 6,  saved_ssid);
    html_escape(e_apn,    64 * 6,  saved_apn);
    html_escape(e_uri,    128 * 6, saved_uri);
    html_escape(e_blank,  64 * 6,  ntp_blank);
    html_escape(e_shown,  64 * 6,  ntp_shown);
    html_escape(e_tenant, 32 * 6,  saved_tenant);

    // Heap buffer for the dynamic HTML (page ~3.3 KB + escaped field values)
    const size_t resp_size = 8192;
    char *resp_str = malloc(resp_size);
    if (!resp_str) {
        free(esc);
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }

    // Inject data into HTML variables
    snprintf(resp_str, resp_size, HTML_CONFIG_PAGE,
             mode == 0 ? "checked" : "",
             mode == 1 ? "checked" : "",
             e_ssid,
             e_apn,
             e_uri,
             e_blank, e_blank, e_shown,
             (unsigned long)(saved_tlm_ms / 1000),
             e_tenant);
    free(esc);

    httpd_resp_set_type(req, "text/html");
    esp_err_t res = httpd_resp_send(req, resp_str, HTTPD_RESP_USE_STRLEN);
    
    free(resp_str);
    return res;
}

static esp_err_t captive_redirect_handler(httpd_req_t *req) {
    httpd_resp_set_status(req, "302 Found");
    httpd_resp_set_hdr(req, "Location", "http://192.168.4.1/");
    return httpd_resp_send(req, NULL, 0);
}

static esp_err_t post_save_handler(httpd_req_t *req) {
    char buf[768];
    size_t total = req->content_len;
    if (total == 0 || total >= sizeof(buf)) {
        httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "Form data empty or too long");
        return ESP_FAIL;
    }
    // The form can arrive in more than one piece: keep reading until all of it
    // is in. (One read used to drop whatever came after the first piece.)
    size_t received = 0;
    while (received < total) {
        int ret = httpd_req_recv(req, buf + received, total - received);
        if (ret == HTTPD_SOCK_ERR_TIMEOUT) continue;   // slow phone: wait for the rest
        if (ret <= 0) return ESP_FAIL;                 // connection closed or error
        received += (size_t)ret;
    }
    buf[received] = '\0';

    char raw_val[128];
    char clean_ssid[64] = {0}, clean_pass[64] = {0}, clean_sec_uri[128] = {0}, clean_tenant[32] = {0}, clean_apn[64] = {0};
    char clean_ntp[64] = {0};
    bool ntp_field_sent = false;
    long tlm_s = -1;
    uint8_t mode_val = 0;

    if (httpd_query_key_value(buf, "hw_mode", raw_val, sizeof(raw_val)) == ESP_OK) mode_val = (uint8_t)atoi(raw_val);
    if (httpd_query_key_value(buf, "ssid", raw_val, sizeof(raw_val)) == ESP_OK) url_decode(clean_ssid, raw_val, sizeof(clean_ssid));
    if (httpd_query_key_value(buf, "pass", raw_val, sizeof(raw_val)) == ESP_OK) url_decode(clean_pass, raw_val, sizeof(clean_pass));
    if (httpd_query_key_value(buf, "apn", raw_val, sizeof(raw_val)) == ESP_OK) url_decode(clean_apn, raw_val, sizeof(clean_apn));
    if (httpd_query_key_value(buf, "sec_uri", raw_val, sizeof(raw_val)) == ESP_OK) url_decode(clean_sec_uri, raw_val, sizeof(clean_sec_uri));
    if (httpd_query_key_value(buf, "tenant_id", raw_val, sizeof(raw_val)) == ESP_OK) url_decode(clean_tenant, raw_val, sizeof(clean_tenant));
    if (httpd_query_key_value(buf, "tlm_s", raw_val, sizeof(raw_val)) == ESP_OK) {
        char *end = NULL;
        long v = strtol(raw_val, &end, 10);
        if (end != raw_val && *end == '\0' && v >= 5 && v <= 3600) tlm_s = v;   // else keep the old value
    }
    if (httpd_query_key_value(buf, "ntp_srv", raw_val, sizeof(raw_val)) == ESP_OK) {
        ntp_field_sent = true;
        url_decode(clean_ntp, raw_val, sizeof(clean_ntp));
    }

    // --- Time server: host name or IP only (letters, digits, '.', '-') ---
    {
        char *p = clean_ntp;
        while (isspace((unsigned char)*p)) p++;
        memmove(clean_ntp, p, strlen(p) + 1);
        size_t n = strlen(clean_ntp);
        while (n > 0 && isspace((unsigned char)clean_ntp[n - 1])) clean_ntp[--n] = '\0';
        for (size_t i = 0; i < n; i++) {
            if (!isalnum((unsigned char)clean_ntp[i]) && clean_ntp[i] != '.' && clean_ntp[i] != '-') {
                clean_ntp[0] = '\0';      // reject, keep the default
                break;
            }
        }
    }

    // --- C-Level Sanitization for Tenant ID ---
    if (strlen(clean_tenant) > 0) {
        // Trim leading spaces
        char *p = clean_tenant;
        while(isspace((unsigned char)*p)) p++;
        memmove(clean_tenant, p, strlen(p) + 1);
        
        // Trim trailing spaces
        char *end = clean_tenant + strlen(clean_tenant) - 1;
        while(end >= clean_tenant && isspace((unsigned char)*end)) {
            *end = '\0';
            end--;
        }

        // Lowercase string and reject forbidden chars natively as a fail-safe
        bool valid_tid = true;
        for(int i = 0; clean_tenant[i]; i++){
            clean_tenant[i] = tolower((unsigned char)clean_tenant[i]);
            if(clean_tenant[i] == '/' || clean_tenant[i] == '+' || clean_tenant[i] == '#' || clean_tenant[i] == ' ') {
                valid_tid = false;
            }
        }
        if (!valid_tid) clean_tenant[0] = '\0'; // Nuke string if illegal chars bypass HTML
    }

    // Values read back from flash after saving (on the heap: this handler
    // already holds ~1.5 KB of buffers on the HTTP server task's stack).
    typedef struct {
        char tenant[32], ssid[64], apn[64], uri[128], ntp[64];
        char e_tenant[32 * 6], e_ssid[64 * 6], e_apn[64 * 6], e_uri[128 * 6], e_ntp[64 * 6];
    } saved_view_t;
    saved_view_t *v = calloc(1, sizeof(saved_view_t));
    if (!v) { httpd_resp_send_500(req); return ESP_FAIL; }
    snprintf(v->tenant, sizeof(v->tenant), "UNREGISTERED");
    uint8_t saved_mode = mode_val;
    uint32_t saved_tlm_ms = 60000;
    bool pass_changed = false;

    nvs_handle_t h;
    if (nvs_open("senseable", NVS_READWRITE, &h) == ESP_OK) {
        nvs_set_u8(h, "hw_mode", mode_val);
        
        if (strlen(clean_ssid) > 0) nvs_set_str(h, "wifi_ssid", clean_ssid);
        if (strlen(clean_pass) > 0) { nvs_set_str(h, "wifi_pass", clean_pass); pass_changed = true; }
        if (strlen(clean_apn) > 0) nvs_set_str(h, "cell_apn", clean_apn);
        if (strlen(clean_sec_uri) > 0) nvs_set_str(h, "sec_uri", clean_sec_uri);
        if (strlen(clean_tenant) > 0) nvs_set_str(h, "tenant_id", clean_tenant);
        // Blank (or rejected) time server = erase it, so the default applies.
        if (strlen(clean_ntp) > 0)  nvs_set_str(h, "ntp_srv", clean_ntp);
        else if (ntp_field_sent)    nvs_erase_key(h, "ntp_srv");
        // Portal interval; clears any remote (dashboard) value so this one applies.
        if (tlm_s > 0) {
            nvs_set_u32(h, "tlm_ms_p", (uint32_t)tlm_s * 1000);
            nvs_erase_key(h, "tlm_ms");
        }
        
        nvs_commit(h); 
        
        // Read back the definitive values to show the user
        size_t len;
        nvs_get_u8(h, "hw_mode", &saved_mode);
        len = sizeof(v->tenant); nvs_get_str(h, "tenant_id", v->tenant, &len);
        len = sizeof(v->ssid);   nvs_get_str(h, "wifi_ssid", v->ssid, &len);
        len = sizeof(v->apn);    nvs_get_str(h, "cell_apn", v->apn, &len);
        len = sizeof(v->uri);    nvs_get_str(h, "sec_uri", v->uri, &len);
        len = sizeof(v->ntp);    nvs_get_str(h, "ntp_srv", v->ntp, &len);
        // Same order the node uses at boot: remote value, else portal value, else 60 s
        if (nvs_get_u32(h, "tlm_ms", &saved_tlm_ms) != ESP_OK) nvs_get_u32(h, "tlm_ms_p", &saved_tlm_ms);
        nvs_close(h);
        ESP_LOGI("PROVISION", "Credentials safely committed to NVS.");
    }

    bool cellular = (saved_mode == 1);
    const char *ntp_shown = v->ntp[0] ? v->ntp
                          : (ntp_server_default[0] ? ntp_server_default : "pool.ntp.org only");
    html_escape(v->e_tenant, sizeof(v->e_tenant), v->tenant);
    html_escape(v->e_ssid,   sizeof(v->e_ssid),   v->ssid);
    html_escape(v->e_apn,    sizeof(v->e_apn),    v->apn);
    html_escape(v->e_uri,    sizeof(v->e_uri),    v->uri[0] ? v->uri : "(not set)");
    html_escape(v->e_ntp,    sizeof(v->e_ntp),    ntp_shown);

    // Allocate buffer for success page injection (page ~1.6 KB + escaped values)
    const size_t resp_size = 4096;
    char *resp_str = malloc(resp_size);
    if (!resp_str) { free(v); httpd_resp_send_500(req); return ESP_FAIL; }

    snprintf(resp_str, resp_size, HTML_SAVED_PAGE,
             node_id,
             v->e_tenant,
             cellular ? "Cellular" : "Wi-Fi",
             cellular ? "Cellular APN" : "Wi-Fi SSID",
             cellular ? v->e_apn : v->e_ssid,
             pass_changed ? "changed (not shown)" : "unchanged (not shown)",
             v->e_uri,
             v->e_ntp,
             (unsigned long)(saved_tlm_ms / 1000));
    free(v);

    httpd_resp_set_type(req, "text/html");
    esp_err_t res = httpd_resp_send(req, resp_str, HTTPD_RESP_USE_STRLEN);
    
    free(resp_str);
    return res;
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