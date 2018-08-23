package com.lma.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountDriver {

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());

        //hadoop jar方式运行
        //设置运行的jar位置
//        job.setJar("/home/hadoop/wordcount.jar");

        //本地运行
        job.setJarByClass(WordCountDriver.class);

        //设置mapper类
        job.setMapperClass(WordCountMapper.class);
        //设置reduce类
        job.setReducerClass(WordCountReduce.class);

        //设置map输出的key类型
        job.setMapOutputKeyClass(Text.class);
        //设置map输入的value类型
        job.setMapOutputValueClass(IntWritable.class);

        //设置全局输出key类型
        job.setOutputKeyClass(Text.class);
        //设置全局输出value类型
        job.setOutputValueClass(IntWritable.class);

        //设置数据读取组件、数据输出组件
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //设置待处理文件位置
//        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        //设置处理结果存储文职
//        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));

        //设置待处理文件位置
        FileInputFormat.setInputPaths(job, new Path("d:/wordcount/input"));
        //设置处理结果存储文职
        FileOutputFormat.setOutputPath(job, new Path("d:/wordcount/output"));

        //提交任务，等待处理完毕
        boolean res = job.waitForCompletion(true);

        System.exit(res ? 0 : 1);
    }
}
