package com.jeninfo.hadoopservice.config;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author chenzhou
 * @date 2018/10/31 09:13
 * @description
 */
@Configuration
public class EsConfig {
    @Bean
    public TransportClient createTransportClient() {
        //设置集群名称
        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        try {
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("47.98.236.124"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;
    }
}
