package com.jeninfo.hadoopservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.config.server.EnableConfigServer;

/**
 * @author chenzhou
 * @date 2018/12/7 13:24
 * @description
 */
@SpringBootApplication
@EnableConfigServer
public class StartSpringCloudConfig {
    public static void main(String[] args) {
        SpringApplication.run(StartSpringCloudConfig.class, args);
    }
}
