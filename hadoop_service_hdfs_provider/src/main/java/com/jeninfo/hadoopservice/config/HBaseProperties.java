package com.jeninfo.hadoopservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author chenzhou
 * @date 2018/10/30 12:08
 * @description
 */
@Component
@ConfigurationProperties(prefix = "hbase")
public class HBaseProperties {
    private Map<String, String> config;

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }
}
