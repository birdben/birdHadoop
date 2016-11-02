package com.birdben.mapreduce.demo;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private Log logger = LogFactory.getLog(IntSumReducer.class);

    IntWritable result = new IntWritable();

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
