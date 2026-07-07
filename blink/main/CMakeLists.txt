idf_component_register(SRCS "blink_example_main.c"
                    INCLUDE_DIRS "."
                    REQUIRES esp_driver_i2c nvs_flash esp_wifi esp_event mqtt esp_netif lwip esp_driver_gpio esp_driver_ledc)