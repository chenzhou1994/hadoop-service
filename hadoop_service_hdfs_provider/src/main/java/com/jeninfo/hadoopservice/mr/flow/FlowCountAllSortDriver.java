package com.jeninfo.hadoopservice.mr.flow;

import com.jeninfo.hadoopservice.mr.bean.FlowBean;
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
 * @author chenzhou
 * <p>
 * 5. 将统计结果按照总流量倒序排序（全排序,实现WritableComparable,重写compareTo方法）
 * <p>
 * 6. 不同省份输出文件内部排序（部分排序,增加自定义分区类）
 */
public class FlowCountAllSortDriver {
    public static boolean flowCountMain(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 6 指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowCountAllSortDriver.class);

        // 2 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(FlowCountAllSortMapper.class);
        job.setReducerClass(FlowCountAllSortReducer.class);

        // 3 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 4 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 5 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        return result;
    }

    /**
     * map阶段
     * 输出格式：<手机号, flowBean('上行流量'，'下行流量','总流量')>
     */
    public static class FlowCountAllSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
        private Text phone = new Text();
        private FlowBean flowBean = new FlowBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 1 拿到的是上一个统计程序输出的结果，已经是各手机号的总流量信息
            String line = value.toString();

            // 2 截取字符串并获取电话号、上行流量、下行流量
            String[] fields = line.split("\t");
            String phoneNbr = fields[0];

            long upFlow = Long.parseLong(fields[1]);
            long downFlow = Long.parseLong(fields[2]);

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
    public static class FlowCountAllSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
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
