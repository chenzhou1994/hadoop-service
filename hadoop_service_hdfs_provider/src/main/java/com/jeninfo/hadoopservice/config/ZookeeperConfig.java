package com.jeninfo.hadoopservice.config;

import com.jeninfo.hadoopservice.properties.ZookeeperProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;


/**
 * @Author chenzhou
 * @Date 2019/3/17 14:24
 * @Description
 */
@Configuration
public class ZookeeperConfig {

    @Autowired
    private ZookeeperProperties zookeeperProperties;

}
