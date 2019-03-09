package com.jeninfo.hadoopservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;

/**
 * @author chenzhou
 *
 */
@SpringBootApplication(exclude={DataSourceAutoConfiguration.class})
@EnableEurekaClient
public class HadoopConsumerTestApplication {

	public static void main(String[] args) {
		SpringApplication.run(HadoopConsumerTestApplication.class, args);
	}
}
