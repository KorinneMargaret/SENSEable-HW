idf_component_register(SRCS "blink_example_main.c"
                       INCLUDE_DIRS "."
                       PRIV_REQUIRES esp_http_server
                                     esp_wifi
                                     esp_event
                                     esp_netif
                                     nvs_flash
                                     esp_driver_i2c
                                     esp_driver_gpio
                                     esp_driver_uart)