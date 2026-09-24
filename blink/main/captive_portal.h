#ifndef CAPTIVE_PORTAL_H
#define CAPTIVE_PORTAL_H

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

// Expose the global node_id so the softAP can append it to the SSID
extern char node_id[32];

void dns_server_task(void *pvParameters);
void start_captive_web_server(void);
void wifi_init_softap(void);

#endif // CAPTIVE_PORTAL_H