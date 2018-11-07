package com.jeninfo.hadoopservice.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author chenzhou
 * @date 2018/11/7 11:38
 * @description
 */
@Component
public class KafkaConsumer {

    @KafkaListener(topics = {"firstTopic"}, id = "0")
    public void receiveMessage(ConsumerRecord<?, ?> record) {
        System.out.println("topic" + record.topic());
        System.out.println("key:" + record.key());
        System.out.println("value:" + record.value());
    }
}
