package com.jeninfo.hadoopservice.controller;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.jeninfo.hadoopservice.model.User;
import com.jeninfo.hadoopservice.service.UserService;
import com.jeninfo.hadoopservice.vo.BaseController;
import com.jeninfo.hadoopservice.vo.Render;
import com.netflix.hystrix.contrib.javanica.annotation.HystrixCommand;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

/**
 * @author chenzhou
 */
@RestController
@RequestMapping(value = "/api/admin/user")
public class UserController extends BaseController {
    @Autowired
    private UserService userService;
    @Autowired
    private DiscoveryClient discoveryClient;

    /**
     * 查询全部
     *
     * @param page
     * @param pageSize
     * @return
     */
    @RequestMapping(value = "/select/all", method = RequestMethod.GET)
    public Render selectAll(@RequestParam(value = "page", required = false, defaultValue = "1") Integer page, @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize) {
        IPage<User> userIPage = userService.selectByPage(page, pageSize);
        return this.renderPage(userIPage.getRecords(), page, pageSize, userIPage.getTotal(), 0x111, "获取成功");
    }

    @RequestMapping(value = "/select/{id}", method = RequestMethod.GET)
    @HystrixCommand(fallbackMethod = "fallbackSelectById")
    public Render selectAll(@PathVariable("id") String id) {
        User user = userService.selectById(id);
        if (user == null) {
            throw new RuntimeException("该ID" + id + "没有对应记录！");
        }
        return this.renderSuccess(userService.selectById(id), 0x1111, "获取成功!");
    }

    public Render fallbackSelectById(@PathVariable("id") String id) {
        return this.renderSuccess(new User().setId(UUID.randomUUID().toString()).setName("该ID" + id + "没有对应记录！").setPwd(""), 0x1111, "获取成功!");
    }
    
    /**
     * 服务发现
     *
     * @return
     */
    @RequestMapping(value = "/select/discovery", method = RequestMethod.GET)
    public Object discovery() {
        List<String> list = discoveryClient.getServices();
        System.out.println(list);
        List<ServiceInstance> srvList = discoveryClient.getInstances("HADOOP-SERVICE-PROVIDER");
        for (ServiceInstance element : srvList) {
            System.out.println(element.getServiceId() + "\t" + element.getHost() + "\t" + element.getPort() + "\t"
                    + element.getUri());
        }
        return this.discoveryClient;
    }
}
