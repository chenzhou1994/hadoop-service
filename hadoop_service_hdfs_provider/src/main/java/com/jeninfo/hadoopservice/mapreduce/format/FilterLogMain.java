package com.jeninfo.hadoopservice.mapreduce.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author chenzhou
 * @Date 2019/3/14 13:07
 * @Description
 */
public class FilterLogMain {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(FilterLogMain.class);
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 要将自定义的输出格式组件设置到job中
        job.setOutputFormatClass(MyFilterOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 虽然我们自定义了outputformat，但是因为我们的outputformat继承自fileoutputformat
        // 而fileoutputformat要输出一个_SUCCESS文件，所以，在这还得指定一个输出目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    /**
     * 自定义outputformat
     */
    public static class MyFilterOutputFormat extends FileOutputFormat<Text, NullWritable> {

        @Override
        public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
            return new MyFilterRecordWriter(job);
        }
    }

    public static class MyFilterRecordWriter extends RecordWriter<Text, NullWritable> {
        FSDataOutputStream atguiguOut = null;
        FSDataOutputStream otherOut = null;

        public MyFilterRecordWriter(TaskAttemptContext job) {
            // 1 获取文件系统
            FileSystem fs;
            try {
                fs = FileSystem.get(job.getConfiguration());

                // 2 创建输出文件路径
                Path atguiguPath = new Path("e:/atguigu.log");
                Path otherPath = new Path("e:/other.log");

                // 3 创建输出流
                atguiguOut = fs.create(atguiguPath);
                otherOut = fs.create(otherPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void write(Text key, NullWritable value) throws IOException, InterruptedException {
            // 判断是否包含“atguigu”输出到不同文件
            if (key.toString().contains("atguigu")) {
                atguiguOut.write(key.toString().getBytes());
            } else {
                otherOut.write(key.toString().getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            // 关闭资源
            if (atguiguOut != null) {
                atguiguOut.close();
            }

            if (otherOut != null) {
                otherOut.close();
            }
        }
    }

    public static class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 1 获取一行
            String line = value.toString();
            k.set(line);
            // 3 写出
            context.write(k, NullWritable.get());
        }
    }

    public static class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {

            String k = key.toString();
            k = k + "\r\n";
            context.write(new Text(k), NullWritable.get());
        }
    }
}
