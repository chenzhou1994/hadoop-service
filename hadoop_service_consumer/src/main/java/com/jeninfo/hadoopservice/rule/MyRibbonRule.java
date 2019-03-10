package com.jeninfo.hadoopservice.rule;

import com.netflix.loadbalancer.IRule;
import com.netflix.loadbalancer.RandomRule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Author chenzhou
 * @Date 2019/3/10 12:09
 * @Description
 */
@Configuration
public class MyRibbonRule {

    @Bean
    public IRule myRule() {
        return new RandomRule();
    }
}
