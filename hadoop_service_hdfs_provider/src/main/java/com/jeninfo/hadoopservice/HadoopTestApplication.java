package com.jeninfo.hadoopservice;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;

/**
 * @author chenzhou
 * springboot启动时会自动注入数据源和配置jpa
 */
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, HibernateJpaAutoConfiguration.class})
public class HadoopTestApplication {

    public static void main(String[] args) {
        SpringApplication.run(HadoopTestApplication.class, args);
    }
}
