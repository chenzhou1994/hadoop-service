package com.jeninfo.hadoopservice.mapreduce.log;

import com.jeninfo.hadoopservice.bean.LogBean;
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
 * @Date 2019/3/14 13:29
 * @Description
 */
public class LogParseMain {

    public static void main(String[] args) throws Exception {
        // 1 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 加载jar包
        job.setJarByClass(LogParseMain.class);

        // 3 关联map
        job.setMapperClass(LogMapper.class);

        // 4 设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 5 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6 提交
        job.waitForCompletion(true);
    }


    public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 1 获取1行
            String line = value.toString();
            // 2 解析日志是否合法
            LogBean bean = pressLog(line);
            if (!bean.isValid()) {
                return;
            }

            k.set(bean.toString());
            // 3 输出
            context.write(k, NullWritable.get());
        }

        /**
         * 解析日志
         *
         * @param line
         * @return
         */
        private LogBean pressLog(String line) {
            LogBean logBean = new LogBean();
            // 1 截取
            String[] fields = line.split(" ");
            if (fields.length > 11) {
                // 2封装数据
                logBean.setRemote_addr(fields[0])
                        .setRemote_user(fields[1])
                        .setTime_local(fields[3].substring(1))
                        .setRequest(fields[6])
                        .setStatus(fields[8])
                        .setBody_bytes_sent(fields[9])
                        .setHttp_referer(fields[10]);
                if (fields.length > 12) {
                    logBean.setHttp_user_agent(fields[11] + " " + fields[12]);
                } else {
                    logBean.setHttp_user_agent(fields[11]);
                }
                // 大于400，HTTP错误
                if (Integer.parseInt(logBean.getStatus()) >= 400) {
                    logBean.setValid(false);
                }
            } else {
                logBean.setValid(false);
            }
            return logBean;
        }
    }

}
