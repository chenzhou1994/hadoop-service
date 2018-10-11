package com.jeninfo.hadoopservice.controller;

import com.jeninfo.hadoopservice.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.util.List;

/**
 * @author chenzhou
 */
@RestController
@RequestMapping(value = "/consumer/user")
public class UserController {
    private static final String REST_URL = "http://localhost:8001";
    // private static final String REST_URL_PREFIX = "http://MICROSERVICECLOUD-DEPT";

    @Autowired
    private RestTemplate restTemplate;

    @RequestMapping(value = "/select/all")
    public List<User> selectAll(@RequestParam String page, @RequestParam String pageSize) {
        Integer p = Integer.parseInt(page);
        Integer ps = Integer.parseInt(pageSize);
        return restTemplate.getForObject(REST_URL + "/admin/user/select/all?page=" + p + "&pageSize=" + ps, List.class);
    }
}
