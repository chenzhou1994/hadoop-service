package com.jeninfo.hadoopservice.mr.video;

import com.jeninfo.hadoopservice.mr.bean.FlowBean;
import com.jeninfo.hadoopservice.mr.bean.VideoBean;
import com.jeninfo.hadoopservice.mr.flow.FlowCountAllSortDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author chenzhou
 */
public class VideoCountDriver {

    public static boolean VideoCountMain(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 6 指定本程序的jar包所在的本地路径
        job.setJarByClass(VideoCountDriver.class);

        // 2 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(VideoCountMapper.class);
        job.setReducerClass(VideoCountReducer.class);

        // 3 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(VideoBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 4 指定最终输出的数据的kv类型
        job.setOutputKeyClass(VideoBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 5 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        return result;
    }

    public static class VideoCountMapper extends Mapper<LongWritable, Text, VideoBean, NullWritable> {
        private VideoBean videoBean = new VideoBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split("\t");
            videoBean.setVideoId(fields[0])
                    .setUploader(fields[1])
                    .setAge(Integer.parseInt(fields[2]))
                    .setCategory(Integer.parseInt(fields[3]))
                    .setLength(Integer.parseInt(fields[4]))
                    .setViews(Integer.parseInt(fields[5]))
                    .setRate(Double.parseDouble(fields[6]))
                    .setRatings(Integer.parseInt(fields[7]))
                    .setConments(Integer.parseInt(fields[8]))
                    .setRelatedIds(fields[9]);
            context.write(videoBean, NullWritable.get());
        }
    }


    public static class VideoCountReducer extends Reducer<VideoBean, NullWritable, VideoBean, NullWritable> {
        @Override
        protected void reduce(VideoBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, values.iterator().next());
        }
    }
}
