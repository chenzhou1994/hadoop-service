package com.jeninfo.hadoopservice.controller;

import com.jeninfo.hadoopservice.Msg;
import com.jeninfo.hadoopservice.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @author chenzhou
 */
@RestController
@RequestMapping(value = "/admin/user")
public class UserController {
    @Autowired
    private UserService userService;
    @Autowired
    private DiscoveryClient discoveryClient;

    @RequestMapping(value = "/select/all", method = RequestMethod.GET)
    public Msg selectAll(@RequestParam(value = "page", required = false, defaultValue = "1") String page, @RequestParam(value = "pageSize", required = false, defaultValue = "10") String pageSize) {
        return Msg.renderSuccess("处理成功", 0x342, userService.selectAll(Integer.parseInt(page), Integer.parseInt(pageSize)));
    }

    @RequestMapping(value = "/select/{id}", method = RequestMethod.GET)
    public Msg selectById(@PathVariable("id") String id) {
        return Msg.renderSuccess("处理成功", 0x342, userService.selectById(id));
    }


    @RequestMapping(value = "/select/discovery", method = RequestMethod.GET)
    public Object discovery() {
        List<String> list = discoveryClient.getServices();
        System.out.println("**********" + list);

        List<ServiceInstance> srvList = discoveryClient.getInstances("HADOOP_SERVICE_PROVIDER");
        for (ServiceInstance element : srvList) {
            System.out.println(element.getServiceId() + "\t" + element.getHost() + "\t" + element.getPort() + "\t"
                    + element.getUri());
        }
        return this.discoveryClient;
    }
}
