package com.jeninfo.hadoopservice.mapreduce.log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author chenzhou
 * @Date 2019/3/14 13:15
 * @Description
 */
public class LogMain {
    public static void main(String[] args) throws Exception {
        // 1 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 加载jar包
        job.setJarByClass(LogMain.class);

        // 3 关联map
        job.setMapperClass(LogMapper.class);

        // 4 设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 5 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("F:\\WORKSPACE\\big_datas\\mr\\web.log"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\WORKSPACE\\big_datas\\mr\\result"));

        // 6 提交
        job.waitForCompletion(true);
    }


    public static class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // 1 获取1行数据
            String line = value.toString();
            // 2 解析日志
            boolean result = parseLog(line, context);
            // 3 日志不合法退出
            if (!result) {
                return;
            }
            // 4 设置key
            k.set(line);
            // 5 写出数据
            context.write(k, NullWritable.get());
        }

        /**
         * 解析日志
         *
         * @param line
         * @param context
         * @return
         */
        private boolean parseLog(String line, Context context) {
            // 1 截取
            String[] fields = line.split(" ");

            // 2 日志长度大于11的为合法
            if (fields.length > 11) {
                // 系统计数器
                context.getCounter("map", "true").increment(1);
                return true;
            } else {
                context.getCounter("map", "false").increment(1);
                return false;
            }
        }
    }
}
