package com.jeninfo.hadoopservice.controller;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.jeninfo.hadoopservice.model.User;
import com.jeninfo.hadoopservice.service.UserService;
import com.jeninfo.hadoopservice.vo.BaseController;
import com.jeninfo.hadoopservice.vo.Render;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.web.bind.annotation.*;

import java.util.List;

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

    @RequestMapping(value = "/select/all", method = RequestMethod.GET)
    public Render selectAll(@RequestParam(value = "page", required = false, defaultValue = "1") Integer page, @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize) {
        IPage<User> userIPage = userService.selectByPage(page, pageSize);
        return this.renderPage(userIPage.getRecords(), page, pageSize, userIPage.getTotal(), 0x111, "获取成功");
    }

    @RequestMapping(value = "/select/discovery", method = RequestMethod.GET)
    public Object discovery() {
        List<String> list = discoveryClient.getServices();
        List<ServiceInstance> srvList = discoveryClient.getInstances("HADOOP_SERVICE_PROVIDER");
        for (ServiceInstance element : srvList) {
            System.out.println(element.getServiceId() + "\t" + element.getHost() + "\t" + element.getPort() + "\t"
                    + element.getUri());
        }
        return this.discoveryClient;
    }
}
