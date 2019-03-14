package com.jeninfo.hadoopservice.mapreduce.order;

import com.jeninfo.hadoopservice.bean.OrderBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author chenzhou
 * @Date 2019/3/14 10:27
 * @Description
 */
public class OrderMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        // 3 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 4 指定最终输出的数据的kv类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 5 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path("F:\\WORKSPACE\\big_datas\\mr\\GroupingComparator.txt"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\WORKSPACE\\big_datas\\mr\\result"));

        job.setGroupingComparatorClass(OrderGroupingComparator.class);
        job.setPartitionerClass(OrderPartitioner.class);
        job.setNumReduceTasks(3);

        // 6 指定本程序的jar包所在的本地路径
        job.setJarByClass(OrderMain.class);

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    /**
     * mapper阶段
     * 输出格式:<OrderBean(订单id,成交金额)，Null>)
     */
    public static class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
        private OrderBean orderBean = new OrderBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 1 获取一行
            String line = value.toString();

            // 2 截取
            String[] fields = line.split("\t");

            orderBean.setOrdeId(Integer.parseInt(fields[0])).setPrice(Double.parseDouble(fields[2]));
            context.write(orderBean, NullWritable.get());
        }
    }

    /**
     * reducer阶段
     */
    public static class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    /**
     * 自定义分区
     */
    public static class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {

        @Override
        public int getPartition(OrderBean key, NullWritable value, int numReduceTasks) {
            return (key.getOrdeId() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    /**
     * 分组对比器
     */
    public static class OrderGroupingComparator extends WritableComparator {

        protected OrderGroupingComparator() {
            super(OrderBean.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {

            OrderBean aBean = (OrderBean) a;
            OrderBean bBean = (OrderBean) b;

            int result;
            if (aBean.getOrdeId() > bBean.getOrdeId()) {
                result = 1;
            } else if (aBean.getOrdeId() < bBean.getOrdeId()) {
                result = -1;
            } else {
                result = 0;
            }
            return result;
        }
    }
}
