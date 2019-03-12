package com.jeninfo.hadoopservice.mapreduce.flow;

import com.jeninfo.hadoopservice.bean.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author chenzhou
 * @Date 2019/3/12 20:37
 * @Description
 */
public class FlowCountSortMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();

        // 2 指定本业务job要使用的mapper/Reducer业务类
        Job job = Job.getInstance(configuration);
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);

        // 3 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 4 指定最终输出的数据的kv类型
        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(Text.class);

        // 5 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(""));
        FileOutputFormat.setOutputPath(job, new Path(""));

        // 6 指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowCountSortMain.class);

        // 6.1 分区
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        job.submit();
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    /**
     * map阶段
     * 输出格式：< 手机号, flowBean('上行流量'，'下行流量','总流量') >
     */
    public static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
        private Text phone = new Text();
        private FlowBean flowBean = new FlowBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 1 拿到的是上一个统计程序输出的结果，已经是各手机号的总流量信息
            String line = value.toString();

            // 2 截取字符串并获取电话号、上行流量、下行流量
            String[] fields = line.split("\t");
            String phoneNbr = fields[0];

            // 4 取出上行流量和下行流量
            long upFlow = Long.parseLong(fields[fields.length - 3]);
            long downFlow = Long.parseLong(fields[fields.length - 2]);

            // 3 封装对象
            flowBean.setUpFlow(upFlow).setDownFlow(downFlow).setSumFlow(upFlow + downFlow);
            phone.set(phoneNbr);

            // 4 输出
            context.write(flowBean, phone);
        }
    }

    /**
     * reduce 阶段
     */
    public static class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
        @Override
        protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(values.iterator().next(), key);
        }
    }

    /**
     * 自定义分区函数
     */
    public static class ProvincePartitioner extends Partitioner<FlowBean, Text> {
        @Override
        public int getPartition(FlowBean flowBean, Text value, int i) {
            int partition = 0;

            String preNum = value.toString().substring(0, 3);

            if (" ".equals(preNum)) {
                partition = 5;
            } else {
                if ("136".equals(preNum)) {
                    partition = 1;
                } else if ("137".equals(preNum)) {
                    partition = 2;
                } else if ("138".equals(preNum)) {
                    partition = 3;
                } else if ("139".equals(preNum)) {
                    partition = 4;
                }
            }
            return partition;
        }
    }
}
