package com.jeninfo.hadoopservice.mybatis;

import com.jeninfo.hadoopservice.dao.UserMapper;
import com.jeninfo.hadoopservice.model.Article;
import com.jeninfo.hadoopservice.model.User;
import com.jeninfo.hadoopservice.service.EsService;
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

    @Test
    public void test01() {
        //List<Index> list = new ArrayList<>();
        ////list.add(new Index.Builder(new Article(1, "中国获租巴基斯坦瓜达尔港2000亩土地 为期43年", "巴基斯坦(瓜达尔港)港务局表示，将把瓜达尔港2000亩土地，长期租赁给中方，用于建设(瓜达尔港)首个经济特区。分析指，瓜港首个经济特区的建立，不但能对巴基斯坦的经济发展模式，产生示范作用，还能进一步提振经济水平。\" +\n" +
        ////        "\t\t\t\t\"据了解，瓜达尔港务局于今年6月完成了1500亩土地的征收工作，另外500亩的征收工作也将很快完成", "http://news.163.com/15/0909/14/B332O90E0001124J.html", new Date(), "青年", "匿名")).build());
        ////list.add(new Index.Builder(new Article(2, "的嗡嗡嗡", "而分为氛围分为氛围发", "http://dwdwndkj.com", new Date(), "中央", "张三")).build());
        //list.add(new Index.Builder(new Article(4, "分为氛围巴基斯坦分为氛围分为发", "封为贵人个人个体", "http://kkjjkhkjg.com", new Date(), "纷纷无法", "王五")).build());
        //esService.bulkIndex(list, "articleindex", "articletype");
        // esService.createSearch("巴基斯坦");

        esService.getDocument("articleindex", "articletype", "AWbFKno499om3Xrll7ry");
        // esService.searchAll();
    }
}
