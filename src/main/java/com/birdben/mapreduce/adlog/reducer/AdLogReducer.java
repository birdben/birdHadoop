package com.birdben.mapreduce.adlog.reducer;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AdLogReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    NullWritable nullWritable = NullWritable.get();

    private Log logger = LogFactory.getLog(AdLogReducer.class);

    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
        for(Text val:values) {
            System.out.println("birdben out reduce val:" + val);
            logger.info("birdben log reduce val:" + val);
            context.write(nullWritable, val);
        }
    }
}