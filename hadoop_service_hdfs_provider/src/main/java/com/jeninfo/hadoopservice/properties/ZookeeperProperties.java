package com.jeninfo.hadoopservice.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @Author chenzhou
 * @Date 2019/3/17 14:23
 * @Description
 */
@Component
@ConfigurationProperties(prefix = "zookeeper")
@Data
public class ZookeeperProperties {
    private Map<String, Object> config;
}
