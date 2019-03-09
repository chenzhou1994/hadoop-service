package com.jeninfo.hadoopservice.controller;

import com.jeninfo.hadoopservice.model.User;
import com.jeninfo.hadoopservice.vo.Render;
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

    @Autowired
    private RestTemplate restTemplate;

    @RequestMapping(value = "/select/all")
    public Render selectAll(@RequestParam Integer page, @RequestParam Integer pageSize) {
        return restTemplate.getForObject(REST_URL + "/api/admin/user/select/all?page=" + page + "&pageSize=" + pageSize, Render.class);
    }

    @RequestMapping(value = "/select/discovery")
    public Object discovery(@RequestParam Integer page, @RequestParam Integer pageSize) {
        return restTemplate.getForObject(REST_URL + "/api/admin/user/select/discovery",Object.class);
    }
}
