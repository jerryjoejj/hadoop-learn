package com.lma.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * ReduceTask调用reduce方法
 * ReduceTask接收到map阶段中所有MapTask输出数据中的一部分
 * ReduceTask将接收到的K-V值按照K分组，然后将一组K-V的K传递给reduce方法的key变量
 * 把这一组K-V中所有的V用一个迭代器传给reduce方法的values
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;

        for(IntWritable v : values) {
            count += v.get();
        }

        context.write(key, new IntWritable(count));
    }
}
