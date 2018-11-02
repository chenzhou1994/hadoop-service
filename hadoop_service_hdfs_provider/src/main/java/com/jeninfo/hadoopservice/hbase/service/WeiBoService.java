package com.jeninfo.hadoopservice.hbase.service;

import com.jeninfo.hadoopservice.service.HbaseService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
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

    @Autowired
    private Configuration configuration;

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
        //创建内容表
        hbaseService.createTable(TABLE_CONTENT, 1, new String[]{"info"});

        // 创建用户关系表
        hbaseService.createTable(TABLE_RELATION, 1, new String[]{"attends", "fans"});

        // 收件箱表
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
        // 内容表rowKey
        long ts = System.currentTimeMillis();
        String contentTableRowkey = userId + "_" + ts;
        // 添加一条记录
        hbaseService.addRow(TABLE_CONTENT, contentTableRowkey, "info", "content", content);

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
        hbaseService.addRow(TABLE_INBOX, fans, "info", userId, ts, contentTableRowkey);
    }

    /**
     *
     * @param userId
     * @param attends
     */
    public void addAttends(String userId, String... attends) {
        // a、在用户关系表中，对当前主动操作的用户id进行添加关注的操作
        //参数过滤:如果没有传递关注的人的uid，则直接返回
        if (attends == null || attends.length <= 0 || userId == null) {
            return;
        }
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
            List<Put> puts = new ArrayList<>();
            //在微博用户关系表中，添加新关注的好友
            Put attendPut = new Put(Bytes.toBytes(userId));
            Arrays.stream(attends).forEach(attend -> {
                //为当前用户添加关注人
                attendPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend), Bytes.toBytes(attend));
                // b、在用户关系表中，对被关注的人的用户id，添加粉丝操作
                //被关注的人，添加粉丝（uid）
                Put fansPut = new Put(Bytes.toBytes(attend));
                fansPut.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(userId), Bytes.toBytes(userId));
                puts.add(fansPut);
            });
            puts.add(attendPut);
            relationTable.put(puts);

            // c、对当前操作的用户的收件箱表中，添加他所关注的人的最近的微博rowkey
            //取得微博内容表
            Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
            Scan scan = new Scan();
            //用于存放扫描出来的我所关注的人的微博rowkey
            List<byte[]> rowkeys = new ArrayList<>();
            Arrays.stream(attends).forEach(attend -> {
                //1002_152321283837374
                //扫描微博rowkey，使用rowfilter过滤器
                RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(attend + "_"));
                scan.setFilter(filter);
                //通过该scan扫描结果
                ResultScanner resultScanner = null;
                try {
                    resultScanner = contentTable.getScanner(scan);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                Iterator<Result> iterator = resultScanner.iterator();
                while (iterator.hasNext()) {
                    Result result = iterator.next();
                    rowkeys.add(result.getRow());
                }
            });
            //将取出的微博rowkey放置于当前操作的这个用户的收件箱表中
            //如果所关注的人，没有一条微博，则直接返回
            if (rowkeys.size() <= 0) {
                return;
            }

            //操作inboxTable
            Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
            Put inboxPut = new Put(Bytes.toBytes(userId));
            for (byte[] rowkey : rowkeys) {
                String rowkeyString = Bytes.toString(rowkey);
                String attendUID = rowkeyString.split("_")[0];
                String attendWeiboTS = rowkeyString.split("_")[1];
                inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(attendUID), Long.valueOf(attendWeiboTS), rowkey);
            }
            inboxTable.put(inboxPut);

            //关闭，释放资源
            inboxTable.close();
            contentTable.close();
            relationTable.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
