package com.example.proxy;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(ProxyProperties.class)
public class CcDeepSeekProxyApplication {

    public static void main(String[] args) {
        SpringApplication.run(CcDeepSeekProxyApplication.class, args);
    }
}