package com.jeninfo.hadoopservice.mapreduce.join;

import com.jeninfo.hadoopservice.bean.TableBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @Author chenzhou
 * @Date 2019/3/14 10:48
 * @Description
 */
public class ReduceJoinMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();

        // 2 指定本业务job要使用的mapper/Reducer业务类
        Job job = Job.getInstance(configuration);
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        // 3 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        // 4 指定最终输出的数据的kv类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 5 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path("F:\\WORKSPACE\\big_datas\\mr\\order"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\WORKSPACE\\big_datas\\mr\\result"));

        // 6 指定本程序的jar包所在的本地路径
        job.setJarByClass(ReduceJoinMain.class);

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        job.submit();
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    /**
     * mapper阶段
     * 输出格式:<pId, TableBean对象>
     */
    public static class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
        private static final String ORDER = "order";

        TableBean bean = new TableBean();
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 1 获取输入文件类型
            FileSplit split = (FileSplit) context.getInputSplit();
            String name = split.getPath().getName();

            // 2 获取输入数据
            String line = value.toString();
            // 3 不同文件分别处理
            if (name.startsWith(ORDER)) {
                // 3.1 切割
                String[] fields = line.split(",");
                // 3.2 封装bean对象
                bean.setOrderId(fields[0])
                        .setPId(fields[1])
                        .setAmount(Integer.parseInt(fields[2]))
                        .setPname("")
                        .setFlag("0");
                k.set(fields[1]);
            } else {
                // 3.3 切割
                String[] fields = line.split(",");
                // 3.4 封装bean对象
                bean.setPId(fields[0])
                        .setAmount(0)
                        .setOrderId("")
                        .setPname(fields[1])
                        .setFlag("1");
                k.set(fields[0]);
            }
            // 4 写出
            context.write(k, bean);
        }
    }

    /**
     * reducer阶段
     */
    public static class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<TableBean> values, Context context) {
            // 1准备存储订单的集合
            ArrayList<TableBean> orderBeans = new ArrayList<>();
            // 2 准备bean对象
            TableBean pdBean = new TableBean();
            values.forEach(item -> {
                if ("0".equals(item.getFlag())) {
                    try {
                        // 拷贝传递过来的每条订单数据到集合中
                        orderBeans.add(item);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    try {
                        // 拷贝传递过来的产品表到内存中
                        pdBean.setPId(item.getPId()).setPname(item.getPname()).setAmount(item.getAmount())
                                .setFlag(item.getFlag()).setOrderId(item.getOrderId());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                orderBeans.forEach(orderBean -> {
                    try {
                        item.setPname(pdBean.getPname());
                        // 4 数据写出去
                        context.write(orderBean, NullWritable.get());
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            });

        }
    }
}
