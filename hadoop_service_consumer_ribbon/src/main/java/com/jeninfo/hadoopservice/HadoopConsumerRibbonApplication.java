package com.jeninfo.hadoopservice;

import com.jeninfo.hadoopservice.property.ServiceProperties;
import com.jeninfo.hadoopservice.rule.MyRibbonRule;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.cloud.netflix.ribbon.RibbonClient;

/**
 * @author chenzhou
 */
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@EnableEurekaClient
@RibbonClient(name = ServiceProperties.HADOOP_SERVICE_PROVIDER, configuration = MyRibbonRule.class)
public class HadoopConsumerRibbonApplication {

    public static void main(String[] args) {
        SpringApplication.run(HadoopConsumerRibbonApplication.class, args);
    }
}
