package com.jeninfo.hadoopservice.mybatis;

import com.jeninfo.hadoopservice.dao.UserMapper;
import com.jeninfo.hadoopservice.model.Article;
import com.jeninfo.hadoopservice.model.User;
import com.jeninfo.hadoopservice.service.EsService;
import com.jeninfo.hadoopservice.service.KafKaService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Index;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @author chenzhou
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class MybatisTest {
    @Autowired
    private EsService esService;
    @Autowired
    private KafKaService kafKaService;

    @Test
    public void test01() {
        kafKaService.send("hahahah");
    }
}
