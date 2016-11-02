package com.birdben.mapreduce.adlog.mapper;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.birdben.mapreduce.adlog.parser.AdLogParser;

public class AdLogTokenizerMapper extends Mapper<Object, Text, NullWritable, Text> {

    private Log logger = LogFactory.getLog(AdLogTokenizerMapper.class);

    NullWritable nullWritable = NullWritable.get();
    Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
        System.out.println("birdben AdLogTokenizerMapper out start");
        logger.info("birdben AdLogTokenizerMapper logger start");

        StringTokenizer itr = new StringTokenizer(value.toString());

        while(itr.hasMoreTokens()) {
            String inputString = itr.nextToken();
            try {
                List<String> logs = AdLogParser.convertLogToAd(inputString);
                for (String log : logs) {
                    word.set(log);
                    value.set(log);
                    System.out.println("birdben out map log:" + log);
                    logger.info("birdben log map log:" + log);
                    context.write(nullWritable, value);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}