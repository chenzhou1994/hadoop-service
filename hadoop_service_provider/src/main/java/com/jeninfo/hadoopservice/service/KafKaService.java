package com.jeninfo.hadoopservice.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * @author chenzhou
 * @date 2018/11/7 11:16
 * @description
 */
@Service
public class KafKaService {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void send(String msg) {
        kafkaTemplate.send("firstTopic", "msg", msg);
    }
}
