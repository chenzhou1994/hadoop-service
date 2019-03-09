package com.jeninfo.hadoopservice.service;

import com.jeninfo.hadoopservice.HadoopServiceProviderApplicationTest;
import com.jeninfo.hadoopservice.model.User;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @Author chenzhou
 * @Date 2019/3/9 15:33
 * @Description
 */
public class UserServiceTest extends HadoopServiceProviderApplicationTest {

    @Autowired
    private UserService userService;

    @Test
    public void test() {
        User user = userService.selectById("1222333");
        System.out.println(user);
    }
}
