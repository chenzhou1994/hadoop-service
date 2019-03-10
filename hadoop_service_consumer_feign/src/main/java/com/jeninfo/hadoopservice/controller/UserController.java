package com.jeninfo.hadoopservice.controller;

import com.jeninfo.hadoopservice.client.ProviderFeignClient;
import com.jeninfo.hadoopservice.property.ServiceProperties;
import com.jeninfo.hadoopservice.vo.Render;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

/**
 * @author chenzhou
 */
@RestController
@RequestMapping(value = "/consumer/user")
public class UserController {

    @Autowired
    private ProviderFeignClient providerFeignClient;

    @RequestMapping(value = "/select/all")
    public Render selectAll(@RequestParam(required = false, defaultValue = "1") Integer page, @RequestParam(required = false, defaultValue = "10") Integer pageSize) {
        return providerFeignClient.selectAll(page, pageSize);
    }
}
