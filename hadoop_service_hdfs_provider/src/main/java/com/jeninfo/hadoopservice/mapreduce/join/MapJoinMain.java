package com.jeninfo.hadoopservice.mapreduce.join;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author chenzhou
 * @Date 2019/3/14 12:42
 * @Description
 */
public class MapJoinMain {
    public static void main(String[] args) throws Exception {
        // 1 获取job信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置加载jar包路径
        job.setJarByClass(MapJoinMain.class);

        // 3 关联map
        job.setMapperClass(DistributedCacheMapper.class);

        // 4 设置最终输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 5 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("F:\\WORKSPACE\\big_datas\\mr\\order\\order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\WORKSPACE\\big_datas\\mr\\result"));

        // 6 加载缓存数据
        job.addCacheFile(new URI("file:///F:/WORKSPACE/big_datas/mr/order/pd.txt"));

        // 7 map端join的逻辑不需要reduce阶段，设置reducetask数量为0
        job.setNumReduceTasks(0);

        // 8 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    public static class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Map<String, String> pdMap = new HashMap<>();
        Text k = new Text();

        @Override
        protected void setup(Context context) throws IOException {

            // 1 获取缓存的文件
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("F:/WORKSPACE/big_datas/mr/order/pd.txt"), "UTF-8"));

            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                // 2 切割
                String[] fields = line.split(",");

                // 3 缓存数据到集合
                pdMap.put(fields[0], fields[1]);
            }

            // 4 关流
            reader.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 1 获取一行
            String line = value.toString();

            // 2 截取
            String[] fields = line.split(",");

            // 3 获取产品id
            String pId = fields[1];

            // 4 获取商品名称
            String pdName = pdMap.get(pId);

            // 5 拼接
            k.set(line + "\t" + pdName);

            // 6 写出
            context.write(k, NullWritable.get());
        }
    }
}
