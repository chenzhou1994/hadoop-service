package com.jeninfo.hadoopservice.mybatis;

import com.jeninfo.hadoopservice.service.UserService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author chenzhou
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class MybatisTest {
    @Autowired
    private UserService userService;

    @Test
    public void test01() {
    }
}
