package com.jeninfo.hadoopservice.controller;

import com.jeninfo.hadoopservice.Msg;
import com.jeninfo.hadoopservice.model.User;
import com.jeninfo.hadoopservice.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author chenzhou
 */
@RestController
@RequestMapping(value = "/admin/user")
public class UserController {
    @Autowired
    private UserService userService;

    @RequestMapping(value = "/select/all", method = RequestMethod.GET)
    public Msg selectAll(@RequestParam String page, @RequestParam String pageSize) {
        return Msg.renderSuccess("处理成功", 0x342, userService.selectAll(Integer.parseInt(page), Integer.parseInt(pageSize)));
    }
}
