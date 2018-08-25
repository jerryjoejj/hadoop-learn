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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;


/**
 * 功能实现---流量汇总
 *
 * @author lma
 */
public class OneStepFlowStatisticsSort {


    /**
     * KEYIN：首行偏移量
     * VALUEIN：一行的文本内容
     * KEYOUT：用户手机号
     * VALUEOUT：流量信息
     *
     * 注意：这里需要使用静态内部类
     */
    public static class OneStepFlowStatisticsSortMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

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
    public static class OneStepFlowStatisticsSortReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

        //将reduce执行的结果缓存到treeMap中
        TreeMap<FlowBean, Text> treeMap = new TreeMap<FlowBean, Text>();

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

            long upFlow = 0L;
            long downFlow = 0L;
            long totalFlow = 0L;

            for (FlowBean bean : values) {
                upFlow += bean.getUpFlow();
                downFlow += bean.getDownFlow();
                totalFlow += bean.getTotalFlow();
            }

            FlowBean v = new FlowBean();

            v.setUpFlow(upFlow);
            v.setDownFlow(downFlow);
            v.setTotalFlow(totalFlow);

            treeMap.put(v, new Text(key.toString()));
            //错误的：每次key的引用地址都是相同的，treeMap也是保存的引用，最后一个key的值会将treeMap中所有value值覆盖
//            treeMap.put(v, key);
        }

        /**
         * 重写cleanup方法，手动输出结果（全局输出）
         * @param context 输出上下文
         * @throws IOException IOException
         * @throws InterruptedException InterruptedException
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Set<Map.Entry<FlowBean, Text>> entries = treeMap.entrySet();
            for (Map.Entry<FlowBean, Text> entry : entries) {
                context.write(entry.getValue(), entry.getKey());
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //设置运行主类
        job.setJarByClass(OneStepFlowStatisticsSort.class);

        //设置mapper类
        job.setMapperClass(OneStepFlowStatisticsSortMapper.class);
        //设置reduce类
        job.setReducerClass(OneStepFlowStatisticsSortReducer.class);

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
        FileInputFormat.setInputPaths(job, new Path("d:/flow/input"));
        //设置处理结果存储文职
        FileOutputFormat.setOutputPath(job, new Path("d:/flow/output"));

        boolean res = job.waitForCompletion(true);

        System.exit(res ? 0 : 1);

    }
}
