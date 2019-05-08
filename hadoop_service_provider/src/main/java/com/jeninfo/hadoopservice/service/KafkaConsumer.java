package com.jeninfo.hadoopservice.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * @author chenzhou
 * @date 2019/5/8 10:02
 * @description
 */
@Component
public class KafkaConsumer {
    @KafkaListener(topics = "calllog")
    public void listen(ConsumerRecord<?, String> record) {
        //判断是否为null
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        if (kafkaMessage.isPresent()) {
            //得到Optional实例中的值
            Object message = kafkaMessage.get();
            System.out.println("消费消息:" + message);
        }
    }
}
