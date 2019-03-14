package com.jeninfo.hadoopservice.mapreduce.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;


/**
 * @Author chenzhou
 * @Date 2019/3/14 12:57
 * @Description
 */
public class LittleFileMain {
    public static void main(String[] args) throws Exception {
        args = new String[]{"e:/input", "e:/output11"};

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(LittleFileMain.class);

        job.setInputFormatClass(WholeFileInputformat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setMapperClass(SequenceFileMapper.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }


    public static class WholeFileInputformat extends FileInputFormat<NullWritable, BytesWritable> {

        @Override
        public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            // 1 定义一个自己的recordReader
            WholeRecordReader recordReader = new WholeRecordReader();

            // 2 初始化recordReader
            recordReader.initialize(split, context);

            return recordReader;
        }

        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;
        }
    }

    public static class WholeRecordReader extends RecordReader<NullWritable, BytesWritable> {
        private FileSplit split;
        private Configuration configuration;

        private BytesWritable value = new BytesWritable();
        private boolean processed = false;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            // 获取传递过来的数据
            this.split = (FileSplit) split;
            configuration = context.getConfiguration();
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (!processed) {
                // 1 定义缓存
                byte[] contents = new byte[(int) split.getLength()];
                // 2 获取文件系统
                Path path = split.getPath();
                FileSystem fs = path.getFileSystem(configuration);
                // 3 读取内容
                FSDataInputStream fis = null;
                try {
                    // 3.1 打开输入流
                    fis = fs.open(path);
                    // 3.2 读取文件内容
                    IOUtils.readFully(fis, contents, 0, contents.length);
                    // 3.3 输出文件内容
                    value.set(contents, 0, contents.length);
                } catch (Exception e) {
                } finally {
                    IOUtils.closeStream(fis);
                }
                processed = true;
                return true;
            }
            return false;
        }

        @Override
        public NullWritable getCurrentKey() throws IOException, InterruptedException {
            return NullWritable.get();
        }

        @Override
        public BytesWritable getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return processed ? 1 : 0;
        }

        @Override
        public void close() throws IOException {
        }
    }

    public static class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
        private Text filenameKey;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 获取切片信息
            InputSplit split = context.getInputSplit();
            // 获取切片路径
            Path path = ((FileSplit) split).getPath();
            // 根据切片路径获取文件名称
            filenameKey = new Text(path.toString());
        }

        @Override
        protected void map(NullWritable key, BytesWritable value, Context context)
                throws IOException, InterruptedException {
            // 文件名称为key
            context.write(filenameKey, value);
        }
    }

}