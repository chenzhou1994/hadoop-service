package com.jeninfo.hadoopservice.mybatis;

import com.google.gson.Gson;
import com.jeninfo.hadoopservice.dao.UserMapper;
import com.jeninfo.hadoopservice.model.Article;
import com.jeninfo.hadoopservice.model.User;
import com.jeninfo.hadoopservice.service.EsService;
import com.jeninfo.hadoopservice.service.KafKaService;
import com.jeninfo.hadoopservice.service.UserService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Index;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author chenzhou
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class MybatisTest {
    @Autowired
    private UserService userService;

    @Autowired
    private RedisTemplate redisTemplate;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Test
    public void test01() {
        //stringRedisTemplate.opsForValue().set("aa", "案件啊哈哈");
        //stringRedisTemplate.expire("aa",1000*10, TimeUnit.MILLISECONDS);
        String index = stringRedisTemplate.opsForList().index("list01", 1);
        System.out.println(index);

        Object o = stringRedisTemplate.opsForHash().get("user", "id");
        System.out.println(o);
    }
}
