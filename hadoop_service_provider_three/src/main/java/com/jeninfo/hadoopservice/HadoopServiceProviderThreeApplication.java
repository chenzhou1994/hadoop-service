package com.jeninfo.hadoopservice;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;

/**
 * @author chenzhou
 */
@MapperScan("com.jeninfo.hadoopservice.dao")
@SpringBootApplication
@EnableEurekaClient
@EnableDiscoveryClient
public class HadoopServiceProviderThreeApplication {

    public static void main(String[] args) {
        SpringApplication.run(HadoopServiceProviderThreeApplication.class, args);
    }
}
