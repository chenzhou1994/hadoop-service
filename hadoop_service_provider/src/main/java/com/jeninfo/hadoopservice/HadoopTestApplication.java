package com.jeninfo.hadoopservice;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;

/**
 * @author chenzhou
 */
@MapperScan("com.jeninfo.hadoopservice.dao")
@SpringBootApplication
@EnableEurekaClient
public class HadoopTestApplication {

    public static void main(String[] args) {
        SpringApplication.run(HadoopTestApplication.class, args);
    }
}
