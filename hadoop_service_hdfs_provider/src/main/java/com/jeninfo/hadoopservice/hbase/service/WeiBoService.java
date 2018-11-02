package com.jeninfo.hadoopservice.hbase.service;

import com.jeninfo.hadoopservice.service.HbaseService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author chenzhou
 * @date 2018/11/1 16:50
 * @description
 */
public class WeiBoService {

    private static final String NS_WEIBO = "ns_weibo";
    private static final String TABLE_CONTENT = "ns_weibo:content";
    private static final String TABLE_RELATION = "ns_weibo:relation";
    private static final String TABLE_INBOX = "ns_weibo:inbox";
    @Autowired
    private HbaseService hbaseService;

    /**
     * 创建命名空间
     */
    public void createNamespace() {
        hbaseService.createNameSpace(NS_WEIBO, "chenzhou");
    }

    /**
     * 创建表
     */
    public void createTable() {
        //创建内容表,列族名:info,列名：content,rowkey:用户id_时间戳,value:微博内容（文字内容，图片URL，视频URL，语音URL）
        hbaseService.createTable(TABLE_CONTENT, 1, new String[]{"info"});

        // 创建关系表,列族名：attends，fans,列名：用户id,rowkey：当前操作人的用户id,value：用户id
        hbaseService.createTable(TABLE_RELATION, 1, new String[]{"attends", "fans"});

        // 列族：info,列：当前用户所关注的人的用户id,value：微博rowkey,rowkey：用户id
        hbaseService.createTable(TABLE_RELATION, 100, new String[]{"info"});
    }

    /**
     * 发布微博
     *
     * @param userId
     * @param content
     */
    public void publishContent(String userId, String content) {
        //a、向微博内容表中添加刚发布的内容，多了一个微博rowkey
        // 组装rowKey
        long ts = System.currentTimeMillis();
        String rowkey = userId + "_" + ts;
        hbaseService.addRow(TABLE_CONTENT, rowkey, "info", "content", content);

        // b、向发布微博人的粉丝的收件箱表中，添加该微博rowkey
        List<String> fans = new ArrayList<>();
        Result fansResult = hbaseService.getRecord(TABLE_RELATION, userId, "fans");
        Cell[] cells = fansResult.rawCells();
        Arrays.stream(cells).forEach(cell -> {
            fans.add(Bytes.toString(CellUtil.cloneValue(cell)));
        });
        //如果没有粉丝，则不需要操作粉丝的收件箱表
        if (fans.size() <= 0) {
            return;
        }
        //向收件箱表放置数据
        hbaseService.addRow(TABLE_INBOX, fans, "info", userId, ts, rowkey);
    }
}
