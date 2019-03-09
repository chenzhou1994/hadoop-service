package com.jeninfo.hadoopservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

/**
 * @author chenzhou
 *
 */
@SpringBootApplication(exclude={DataSourceAutoConfiguration.class})
public class HadoopConsumerTestApplication {

	public static void main(String[] args) {
		SpringApplication.run(HadoopConsumerTestApplication.class, args);
	}
}
