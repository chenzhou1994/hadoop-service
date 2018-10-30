package com.jeninfo.hadoopservice.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * @author chenzhou
 * @date 2018/10/30 11:22
 * @description
 */
@Configuration
public class HbaseConfig {
    @Autowired
    private HBaseProperties properties;

    @Bean
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        Map<String, String> config = properties.getConfig();
        config.forEach((K, V) -> {
            configuration.set(K, V);
        });
        return configuration;
    }
}
