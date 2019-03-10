package com.jeninfo.hadoopservice.service;

import com.jeninfo.hadoopservice.HdfsTestApplication;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @Author chenzhou
 * @Date 2019/3/10 17:33
 * @Description
 */
public class HdfsServiceTest extends HdfsTestApplication {
    @Autowired
    private HdfsService hdfsService;

    @Test
    public void test() throws Exception {
        List<LocatedFileStatus> fileStatusList = hdfsService.readFile("/");
        fileStatusList.stream().forEach(status -> {
            System.out.println("文件名：" + status.getPath().getName());
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
