package com.lma.flow;

import org.apache.commons.lang.StringUtils;
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
 * 功能实现---流量汇总
 *
 * @author lma
 */
public class FlowStatistics {


    /**
     * KEYIN：首行偏移量
     * VALUEIN：一行的文本内容
     * KEYOUT：用户手机号
     * VALUEOUT：流量信息
     *
     * 注意：这里需要使用静态内部类
     */
    public static class FlowStatisticsMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

        Text k = new Text();
        FlowBean v = new FlowBean();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = StringUtils.split(line, "\t");
            String phoneName = fields[1];
            long upFlow = Long.parseLong(fields[fields.length - 3]);
            long downFlow = Long.parseLong(fields[fields.length - 2]);

            k.set(phoneName);
            v.setUpFlow(upFlow);
            v.setDownFlow(downFlow);
            v.setTotalFlow(upFlow + downFlow);

            context.write(k, v);

        }
    }


    /**
     *
     */
    public static class FlowStatisticsReducer extends Reducer<Text, FlowBean, Text, FlowBean> {


        /**
         * reduce方法
         * @param key 手机号
         * @param values K-V中K对应的一组V的迭代器
         * @param context context
         * @throws IOException IOException
         * @throws InterruptedException InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context)
                throws IOException, InterruptedException {

            FlowBean v = new FlowBean();

            for (FlowBean bean : values) {
                v.setUpFlow(bean.getUpFlow());
                v.setDownFlow(bean.getDownFlow());
                v.setTotalFlow(bean.getTotalFlow());
            }
            context.write(key, v);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //设置运行主类
        job.setJarByClass(FlowStatistics.class);

        //设置mapper类
        job.setMapperClass(FlowStatisticsMapper.class);
        //设置reduce类
        job.setReducerClass(FlowStatisticsReducer.class);

        //设置map输出的key类型
        job.setMapOutputKeyClass(Text.class);
        //设置map输入的value类型
        job.setMapOutputValueClass(FlowBean.class);

        //设置全局输出key类型
        job.setOutputKeyClass(Text.class);
        //设置全局输出value类型
        job.setOutputValueClass(FlowBean.class);

        //设置数据读取组件、数据输出组件
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //设置待处理文件位置
        FileInputFormat.setInputPaths(job, new Path("/flow/input"));
        //设置处理结果存储文职
        FileOutputFormat.setOutputPath(job, new Path("/flow/output"));

        boolean res = job.waitForCompletion(true);

        System.exit(res?0:1);

    }
}
