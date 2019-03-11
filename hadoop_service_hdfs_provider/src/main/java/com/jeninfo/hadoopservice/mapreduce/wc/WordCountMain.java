package com.jeninfo.hadoopservice.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Author chenzhou
 * @Date 2019/3/11 20:19
 * @Description
 */
public class WordCountMain {
    public static void main(String[] args) throws Exception {
        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();

        // 2 指定本业务job要使用的mapper/Reducer业务类
        Job job = Job.getInstance(configuration);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 3 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 4 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 5 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6 指定本程序的jar包所在的本地路径
        job.setJarByClass(WordCountMain.class);

        // 6.1 设置分区
        job.setPartitionerClass(WordCountPartitioner.class);
        job.setNumReduceTasks(2);

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        job.submit();
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
