package com.jeninfo.hadoopservice.mr.join;

import com.jeninfo.hadoopservice.mr.bean.TableBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.springframework.beans.BeanUtils;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author chenzhou
 * 8. reduce端表合并（数据倾斜）
 */
public class ReduceJoinDriver {

    /**
     * mapper阶段
     * 输出格式:<pId, TableBean对象>
     */
    public static class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
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
            if (name.startsWith("order")) {
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
                        .setPname(fields[1])
                        .setFlag("1")
                        .setAmount(0)
                        .setOrderId("");

                k.set(fields[0]);
            }
        }
    }

    /**
     * reducer阶段
     */
    public static class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
            // 1准备存储订单的集合
            ArrayList<TableBean> orderBeans = new ArrayList<>();
            // 2 准备bean对象
            TableBean pdBean = new TableBean();
            values.forEach(item -> {
                if ("0".equals(item.getFlag())) {
                    try {
                        // 拷贝传递过来的每条订单数据到集合中
                        TableBean orderBean = new TableBean();
                        BeanUtils.copyProperties(orderBean, item);
                        orderBeans.add(orderBean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    try {
                        // 拷贝传递过来的产品表到内存中
                        BeanUtils.copyProperties(pdBean, item);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            orderBeans.forEach(item -> {
                try {
                    item.setPname(pdBean.getPname());
                    // 4 数据写出去
                    context.write(item, NullWritable.get());
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
    }
}
