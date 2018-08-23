package com.lma.flow;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

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
     */
    public class FlowStatisticsMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

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

    public class FlowStatisticsReduce extends Reducer {
        @Override
        protected void reduce(Object key, Iterable values, Context context)
                throws IOException, InterruptedException {
        }
    }

    public static void main(String[] args) {

    }
}
