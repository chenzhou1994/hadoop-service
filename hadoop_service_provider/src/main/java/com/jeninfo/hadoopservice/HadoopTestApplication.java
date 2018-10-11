package com.jeninfo.hadoopservice;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author chenzhou
 *
 */
@MapperScan("com.jeninfo.hadoopservice.dao")
@SpringBootApplication
public class HadoopTestApplication {

	public static void main(String[] args) {
		SpringApplication.run(HadoopTestApplication.class, args);
	}
}
