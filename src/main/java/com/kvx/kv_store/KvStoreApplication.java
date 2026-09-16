package com.kvx.kv_store;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KvStoreApplication {
    public static void main(String[] args) {
        SpringApplication.run(KvStoreApplication.class, args);
    }
}