package com.birdben.mapreduce.demo;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce过程需要继承org.apache.hadoop.mapreduce包中 Reducer 类，并 重写 reduce方法。
 * public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
 * 其中的模板参数同Map一样：
 * 第一个Object表示输入key的类型；
 * 第二个Text表示输入value的类型；
 * 第三个Text表示表示输出键的类型；
 * 第四个IntWritable表示输出值的类型。
 */
public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private Log logger = LogFactory.getLog(IntSumReducer.class);

    IntWritable result = new IntWritable();

    /**
     * reduce 方法的输入参数 key 为单个单词，而 values 是由各Mapper上对应单词的计数值所组成的列表
     *（一个实现了 Iterable 接口的变量，可以理解成 values 里包含若干个 IntWritable 整数，可以通过迭代的方式遍历所有的值），例如：<Hello, list(1, 1, 1)>
     * 所以只要遍历 values 并求和，即可得到某个单词出现的总次数。
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
	    System.out.println("birdben IntSumReducer out start");
	    logger.info("birdben IntSumReducer logger start");
        int sum = 0;
        for(IntWritable val:values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key,result);
    }
}
