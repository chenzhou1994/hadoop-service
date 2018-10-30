package com.jeninfo.hadoopservice.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author chenzhou
 * @date 2018/10/30 12:48
 * @description
 */
@Service
public class HbaseService {

    @Autowired
    private Configuration configuration;


    /**
     * 判断表是否存在
     *
     * @param tableName
     * @return
     */
    public boolean isTableExits(String tableName) {
        boolean exists = false;
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            exists = admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return exists;
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param columnFamily
     * @return
     */
    public boolean createTable(String tableName, String... columnFamily) {
        boolean flag = false;
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            if (!isTableExits(tableName)) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
                Arrays.stream(columnFamily).forEach(column -> {
                    hTableDescriptor.addFamily(new HColumnDescriptor(column));
                });
                admin.createTable(hTableDescriptor);
                flag = true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return flag;
    }

    /**
     * 删除表
     *
     * @param tableName
     * @return
     */
    public boolean deleteTable(String tableName) {
        boolean flag = false;
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            if (isTableExits(tableName)) {
                if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
                    admin.disableTable(TableName.valueOf(tableName));
                }
                admin.deleteTable(TableName.valueOf(tableName));
                flag = true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return flag;
    }

    /**
     * 添加一行
     *
     * @param tableName
     * @param rowKey
     * @param columFamily
     * @param column
     * @param value
     */
    public void addRow(String tableName, String rowKey, String columFamily, String column, String value) {
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columFamily), Bytes.toBytes(column), Bytes.toBytes(value));
            table.put(put);
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

    /**
     * @param tableName
     */
    public void scanTable(String tableName) {
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            scanner.forEach(item -> {
                Cell[] cells = item.rawCells();
                Arrays.stream(cells).forEach(cell -> {
                    System.out.println("行键: " + Bytes.toString(CellUtil.cloneRow(cell)));
                    System.out.println("列族: " + Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.println("列: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println("值: " + Bytes.toString(CellUtil.cloneValue(cell)));

                });
            });
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

    /**
     * @param tableName
     */
    public void getRow(String tableName, String rowKey) {
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            Cell[] cells = result.rawCells();
            Arrays.stream(result.rawCells()).forEach(cell -> {
                System.out.println("行键: " + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("列族: " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值: " + Bytes.toString(CellUtil.cloneValue(cell)));
            });
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
