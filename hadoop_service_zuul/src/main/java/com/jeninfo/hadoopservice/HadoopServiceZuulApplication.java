package com.jeninfo.hadoopservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.cloud.netflix.zuul.EnableZuulProxy;

/**
 * @author chenzhou
 */
@SpringBootApplication(exclude={DataSourceAutoConfiguration.class})
@EnableZuulProxy
public class HadoopServiceZuulApplication {

    public static void main(String[] args) {
        SpringApplication.run(HadoopServiceZuulApplication.class, args);
    }
}
