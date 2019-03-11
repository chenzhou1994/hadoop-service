package com.jeninfo.hadoopservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;

/**
 * @Author chenzhou
 * @Date 2019/3/10 17:27
 * @Description
 */
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@EnableEurekaClient
@EnableDiscoveryClient
public class HdfsServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(HdfsServiceApplication.class, args);
    }
}
