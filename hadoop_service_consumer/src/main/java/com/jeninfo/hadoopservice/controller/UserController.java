package com.jeninfo.hadoopservice.controller;

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
    private static final String REST_URL_PREFIX = "http://" + ServiceProperties.HADOOP_SERVICE_PROVIDER;

    @Autowired
    private RestTemplate restTemplate;

    @RequestMapping(value = "/select/all")
    public Render selectAll(@RequestParam(required = false, defaultValue = "1") Integer page, @RequestParam(required = false, defaultValue = "10") Integer pageSize) {
        return restTemplate.getForObject(REST_URL_PREFIX + "/api/admin/user/select/all?page=" + page + "&pageSize=" + pageSize, Render.class);
    }

    @RequestMapping(value = "/select/discovery")
    public Object discovery(@RequestParam Integer page, @RequestParam Integer pageSize) {
        return restTemplate.getForObject(REST_URL_PREFIX + "/api/admin/user/select/discovery", Object.class);
    }
}
