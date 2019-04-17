package com.jeninfo.hadoopservice.config;

import org.apache.hadoop.fs.FileSystem;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Author chenzhou
 * @Date 2019/3/10 17:30
 * @Description
 */
@Configuration
public class HdfsConfig {
    @Bean
    public FileSystem fileSystem() {
        FileSystem hdfs = null;
        try {
            //获取配置文件信息
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            conf.set("fs.defaultFS", "file:///");
            // conf.set("fs.defaultFS", "hdfs://192.168.1.221:9000");
            // System.setProperty("HADOOP_USER_NAME", "root");
            //获取文件系统
            hdfs = FileSystem.get(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return hdfs;
    }
}
