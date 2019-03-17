package com.jeninfo.hadoopservice.service;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @Author chenzhou
 * @Date 2019/3/17 14:27
 * @Description
 */
public class ZkClientTest {
    private String connectionString = "192.168.74.153:2181,192.168.74.154:2181,192.168.74.155:2181";
    private Integer timeOut = 2000;
    private ZooKeeper zkClient;

    @Before
    public void initZkClient() throws IOException {
        zkClient = new ZooKeeper(connectionString, timeOut, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getType() + "------->>>>" + watchedEvent.getPath());
            }
        });
    }

    /**
     * 创建节点
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        String s = zkClient.create("/cz2", "nihao，wuying，chenzhou".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(s);
    }

    /**
     * 获取子节点
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void selectChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", false);
        children.stream().forEach(System.out::println);
    }

    @Test
    public void isExists() throws KeeperException, InterruptedException {
        Stat exists = zkClient.exists("/cz", false);
        System.out.println(exists == null ? false : true);
    }
}
