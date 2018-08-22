package com.lma.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *     KEYIN：框架读取到数据的key类型--在默认的读取组件InputFormat下，读取的key是一行文本的偏移量，则key的类型为Long类型
 *     VALUEIN：指框架读取到的数据的value类型--在默认的读取组件InputFormat下，读取到的value就是一行文本内容，则value的类型为String类型
 *     KEYOUT：指用户自定义逻辑方法返回的数据中key的类型，由用户业务逻辑决定--在本例中输出的key是单词，所以这里是String类型
 *     VALUEOUT：指用户自定义逻辑方法返回的数据中value的类型，由用户业务逻辑决定--在本例中输出的value是单词数量，所以这里是Integer
 *
 * JDK自定义的类型在序列化时效率较低，因此hadoop自定义了一套数据结构
 *
 * Long -- LongWritable
 * String -- Text
 * Integer -- IntWritable
 * null -- NullWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * map方法为MapReduce程序中被主程序MapTask所调用的业务逻辑方法
     * MapTask会驱动数据读取组件InputFormat读取数据(KEYIN, VALUEIN)，每读取一次(K,V)就会传入到用户写的map方法中执行一次
     * 在默认的InputFormat实现中，此处的key指的是一行的偏移量，value就是一行的内容
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {

        //获取每一行
        String lines = value.toString();
        //对每一行进行切割
        String[] words = lines.split(" ");

        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
