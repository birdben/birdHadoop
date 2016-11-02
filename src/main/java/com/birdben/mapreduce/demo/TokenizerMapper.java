package com.birdben.mapreduce.demo;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map过程需要继承org.apache.hadoop.mapreduce包中 Mapper 类，并重写其map方法。
 * public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
 * 其中的模板参数：
 * 第一个Object表示输入key的类型；
 * 第二个Text表示输入value的类型；
 * 第三个Text表示表示输出键的类型；
 * 第四个IntWritable表示输出值的类型。
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    IntWritable one = new IntWritable(1);
    Text word = new Text();

    /**
     * 作为map方法输入的键值对，其value值存储的是文本文件中的一行（以回车符为行结束标记），而key值为该行的首字母相对于文本文件的首地址的偏移量。
     * 然后StringTokenizer类将每一行拆分成为一个个的单词，并将<word,1>作为map方法的结果输出，其余的工作都交有 MapReduce框架处理。
     * 注：StringTokenizer是Java工具包中的一个类，用于将字符串进行拆分——默认情况下使用空格作为分隔符进行分割。
     */
    public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while(itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}