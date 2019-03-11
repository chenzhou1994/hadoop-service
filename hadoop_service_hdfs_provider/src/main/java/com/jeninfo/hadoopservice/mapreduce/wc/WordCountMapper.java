package com.jeninfo.hadoopservice.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author chenzhou
 * @Date 2019/3/11 20:06
 * @Description
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private IntWritable one = new IntWritable(1);

    /**
     * 每一行输入调用一次map
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        //一行中的单词
        String[] words = s.split(" ");

        // 输出为<word,1>
        for (String word : words) {
            context.write(new Text(word), one);
        }
    }
}
