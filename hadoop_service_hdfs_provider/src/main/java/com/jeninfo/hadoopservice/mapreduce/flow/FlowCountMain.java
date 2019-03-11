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
 * @Date 2019/3/11 20:47
 * @Description 统计每一个手机号耗费的总上行流量、下行流量、总流量, 按照手机归属地不同省份输出到不同文件
 */
public class FlowCountMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();

        // 2 指定本业务job要使用的mapper/Reducer业务类
        Job job = Job.getInstance(configuration);
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        // 3 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 4 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 5 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6 指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowCountMain.class);

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
    public static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        private Text phone = new Text();
        private FlowBean flowBean = new FlowBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 1 将一行内容转成string
            String ling = value.toString();

            // 2 切分字段
            String[] fields = ling.split("\t");

            // 3 取出手机号码
            String phoneNum = fields[1];

            // 4 取出上行流量和下行流量
            long upFlow = Long.parseLong(fields[fields.length - 3]);
            long downFlow = Long.parseLong(fields[fields.length - 2]);

            // 5 写出数据
            phone.set(phoneNum);
            flowBean.setUpFlow(upFlow).setDownFlow(downFlow).setSumFlow(upFlow + downFlow);
            context.write(phone, flowBean);
        }
    }

    /**
     * reduce 阶段
     */
    public static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long sum_upFlow = 0;
            long sum_downFlow = 0;

            // 1 遍历所用bean，将其中的上行流量，下行流量分别累加
            for (FlowBean bean : values) {
                sum_upFlow += bean.getUpFlow();
                sum_downFlow += bean.getDownFlow();
            }

            // 2 封装对象
            FlowBean resultBean = new FlowBean(sum_upFlow, sum_downFlow);
            context.write(key, resultBean);
        }
    }

    /**
     * 自定义分区函数
     */
    public static class ProvincePartitioner extends Partitioner<Text, FlowBean> {
        @Override
        public int getPartition(Text key, FlowBean flowBean, int i) {
            // 1 获取电话号码的前三位
            String preNum = key.toString().substring(0, 3);

            int partition = 4;

            // 2 判断是哪个省
            if ("136".equals(preNum)) {
                partition = 0;
            } else if ("137".equals(preNum)) {
                partition = 1;
            } else if ("138".equals(preNum)) {
                partition = 2;
            } else if ("139".equals(preNum)) {
                partition = 3;
            }
            return partition;
        }
    }
}
