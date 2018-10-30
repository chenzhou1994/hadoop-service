package com.jeninfo.hadoopservice.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
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

    public Connection getConnection() {
        org.apache.hadoop.conf.Configuration configuration = getConfiguration();
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
