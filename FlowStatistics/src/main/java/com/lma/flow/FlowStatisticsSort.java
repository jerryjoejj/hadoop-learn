package com.lma.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


/**
 * 功能实现---流量汇总后排序
 *
 * @author lma
 */
public class FlowStatisticsSort {


    /**
     * KEYIN：首行偏移量
     * VALUEIN：一行的文本内容
     * KEYOUT：流量信息
     * VALUEOUT：手机号
     *
     * 注意：这里需要使用静态内部类
     */
    public static class FlowStatisticsSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

        FlowBean k = new FlowBean();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");

            String phoneNum = fields[0];
            long upFlow = Long.parseLong(fields[1]);
            long downFlow = Long.parseLong(fields[2]);


            v.set(phoneNum);
            k.setUpFlow(upFlow);
            k.setDownFlow(downFlow);
            k.setTotalFlow(upFlow + downFlow);

            context.write(k, v);

        }
    }


    /**
     * KEYIN：流量信息
     * VALUEIN：手机号
     * KEYOUT：手机号
     * VALUEOUT：流量信息
     */
    public static class FlowStatisticsSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

        @Override
        protected void reduce(FlowBean key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //直接将结果写出
            context.write(values.iterator().next(), key);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //设置运行主类
        job.setJarByClass(FlowStatisticsSort.class);

        //设置mapper类
        job.setMapperClass(FlowStatisticsSortMapper.class);
        //设置reduce类
        job.setReducerClass(FlowStatisticsSortReducer.class);

        //设置map输出的key类型
        job.setMapOutputKeyClass(FlowBean.class);
        //设置map输入的value类型
        job.setMapOutputValueClass(Text.class);

        //设置全局输出key类型
        job.setOutputKeyClass(Text.class);
        //设置全局输出value类型
        job.setOutputValueClass(FlowBean.class);

        //设置数据读取组件、数据输出组件
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //设置待处理文件位置
        FileInputFormat.setInputPaths(job, new Path("/flow/output"));
        //设置处理结果存储文职
        FileOutputFormat.setOutputPath(job, new Path("/flow/sortout"));

        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);

    }
}
