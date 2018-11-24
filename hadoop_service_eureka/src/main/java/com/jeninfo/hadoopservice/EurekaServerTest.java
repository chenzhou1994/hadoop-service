package com.jeninfo.hadoopservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

/**
 * @author chenzhou
 * @date 2018/11/24 14:25
 * @description urekaServer服务器端启动类,接受其它微服务注册进来
 */
@SpringBootApplication
@EnableEurekaServer
public class EurekaServerTest {
    public static void main(String[] args) {
        SpringApplication.run(EurekaServerTest.class, args);
    }
}
