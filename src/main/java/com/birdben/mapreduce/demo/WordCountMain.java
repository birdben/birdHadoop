package com.birdben.mapreduce.demo;

import com.birdben.mapreduce.demo.mapper.TokenizerMapper;
import com.birdben.mapreduce.demo.reducer.IntSumReducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountMain {

    private static Log logger = LogFactory.getLog(WordCountMain.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        System.out.println("birdben out WordCountMain start");
        logger.info("birdben logger WordCountMain start");
        
	    // 设置一个用户定义的job名称
        Job job = new Job(conf, "wordcount");
        job.setJarByClass(WordCountMain.class);
        // 为job设置使用的Mapper类（拆分）
        job.setMapperClass(TokenizerMapper.class);
        // 为job设置使用的Combiner类（中间结果合并）
        job.setCombinerClass(IntSumReducer.class);
        // 为job设置使用的Reducer类（合并）
        job.setReducerClass(IntSumReducer.class);
        // 设置了Map过程和Reduce过程的输出类型：key的类型为Text，value的类型为IntWritable
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 任务的输出和输入路径则由命令行参数指定，并由FileInputFormat和FileOutputFormat分别设定。
        // 为map-reduce job设置传入的第一个参数为MapReduce输入的HDFS文件路径
        // FileInputFormat类的很重要的作用就是将文件进行切分split，并将split进一步拆分成key/value对
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // 为map-reduce job设置传入的第二个参数为MapReduce结果输出的HDFS文件路径
        // FileOutputFormat类的作用是将处理结果写入输出文件。
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        // 完成相应任务的参数设定后，调用 job.waitForCompletion() 方法执行任务
        System.exit(job.waitForCompletion(true)?0:1);
    } 
}
