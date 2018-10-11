package com.jeninfo.hadoopservice;

import com.jeninfo.hadoopservice.mr.wc.WordCountDriver;
import com.jeninfo.hadoopservice.service.HdfsService;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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

    @Test
    public void test01() throws Exception {
        // testReadFile();
        String[] args = {"G:\\big_datas\\mr\\wc.txt", "G:\\result"};
        WordCountDriver.wcmain(args);
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
