package com.jeninfo.hadoopservice.mr.order;

import com.jeninfo.hadoopservice.mr.bean.OrderBean;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author chenzhou
 */
public class OrderDriver {

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

    public static class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {

        @Override
        public int getPartition(OrderBean key, NullWritable value, int numReduceTasks) {
            return (key.getOrdeId() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }


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
