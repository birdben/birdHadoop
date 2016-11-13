package com.birdben.storm.demo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class WordCounter extends BaseBasicBolt {
    private static final long serialVersionUID = 5683648523524179434L;
    private FileOutputStream out = null;
    private HashMap<String, Integer> counters = new HashMap<String, Integer>();

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
        System.out.println("WordCounter prepare out start");
        String outputPath = (String) stormConf.get("OUTPUT_PATH");
        try {
            out = new FileOutputStream(new File(outputPath + File.separator + "output_WordCount"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        System.out.println("WordCounter execute out start");
        String str = input.getString(0);
        //System.out.println("WordCounter execute:" + str);
        if (!counters.containsKey(str)) {
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }
        System.out.println("WordCounter execute out end");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    /**
     * Topology执行完毕的清理工作，比如关闭连接、释放资源等操作都会写在这里
     * 这里cleanup方法一般不会被用到的，因为线上Storm集群基本上不会停用的，因为我们这里只是个Demo，所以我们用它来打印我们的计数器
     **/
    @Override
    public void cleanup() {
        System.out.println("WordCounter clean out start");
        try {
            for (Entry<String, Integer> entry : counters.entrySet()) {
                System.out.println("WordCounter result : " + entry.getKey() + " " + entry.getValue());
                out.write((entry.getKey() + " " + entry.getValue() + "\n").getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (out != null) {
                try {
                    out.flush();
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("WordCounter clean out end");
    }
}