package com.jeninfo.hadoopservice.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author chenzhou
 * @Date 2019/3/11 20:32
 * @Description 根据首字母的ASCII分区
 */
public class WordCountPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
        int result = text.toString().charAt(0);
        if (result % 2 == 0) {
            return 0;
        } else {
            return 1;
        }
    }
}
