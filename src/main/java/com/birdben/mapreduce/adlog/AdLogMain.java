package com.birdben.mapreduce.adlog;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import com.birdben.mapreduce.adlog.mapper.AdLogTokenizerMapper;
import com.birdben.mapreduce.adlog.reducer.AdLogReducer;

public class AdLogMain {

    private static Log logger = LogFactory.getLog(AdLogMain.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2) {
            System.err.println("Usage: adlog <in> <out>");
            System.exit(2);
        }

        System.out.println("birdben out AdLog start");
        logger.info("birdben logger AdLog start");

        Job job = new Job(conf, "adlog");
        job.setJarByClass(AdLogMain.class);
        job.setMapperClass(AdLogTokenizerMapper.class);
        job.setCombinerClass(AdLogReducer.class);
        job.setReducerClass(AdLogReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    } 
}
