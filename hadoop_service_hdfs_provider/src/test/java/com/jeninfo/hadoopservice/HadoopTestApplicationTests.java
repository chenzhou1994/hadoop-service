package com.jeninfo.hadoopservice;

import com.jeninfo.hadoopservice.config.HBaseProperties;
import com.jeninfo.hadoopservice.mr.flow.FlowCountAllSortDriver;
import com.jeninfo.hadoopservice.mr.flow.FlowCountDriver;
import com.jeninfo.hadoopservice.mr.order.OrderDriver;
import com.jeninfo.hadoopservice.mr.video.VideoCountDriver;
import com.jeninfo.hadoopservice.mr.wc.WordCountDriver;
import com.jeninfo.hadoopservice.service.HbaseService;
import com.jeninfo.hadoopservice.service.HdfsService;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author chenzhou
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class HadoopTestApplicationTests {
    @Autowired
    private HdfsService hdfsService;
    @Autowired
    private FileSystem fileSystem;
    @Autowired
    private HbaseService hbaseService;

    @Test
    public void test01() throws Exception {
        // System.out.println("====" + configuration.get("hbase.zookeeper.quorum"));
        //boolean exits = hbaseService.isTableExits("cz");
        //boolean user = hbaseService.createTable("user", new String[]{"info_one", "info_two"});
        //System.out.println("===>" + user);
        //hbaseService.addRow("user", "1001", "info_one", "name", "张三");
        hbaseService.scanTable("user");
    }

    private void testReadFile() throws Exception {
        hdfsService.readFile("G:\\big_datas").stream().forEach(status -> {
            System.out.println("文件名===>" + status.getPath().getName());
            // 块的大小
            System.out.println("块的大小：" + status.getBlockSize());
            // 文件权限
            System.out.println("文件权限：" + status.getPermission());
            // 文件块的具体信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            Arrays.stream(blockLocations).forEach(block -> {
                System.out.println("文件所在块：" + block.getOffset());
                System.out.print("\t块主机：");
                String[] hosts = new String[0];
                try {
                    hosts = block.getHosts();
                    Arrays.stream(hosts).forEach(host -> {
                        System.out.print(host + "\t");
                    });
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            System.out.println("\n-------------------");
        });
    }
}
