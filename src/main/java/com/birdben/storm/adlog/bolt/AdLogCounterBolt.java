package com.birdben.storm.adlog.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AdLogCounterBolt extends BaseBasicBolt {

    private FileOutputStream out = null;
    private List<String> logs = new ArrayList();

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
        System.out.println("AdLogCounterBolt prepare out start");
        String outputPath = (String) stormConf.get("OUTPUT_PATH");
        try {
            out = new FileOutputStream(new File(outputPath + File.separator + "output_AdLog"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        System.out.println("AdLogCounterBolt execute out start");
        String log = input.getString(0);
        //System.out.println("AdLogCounterBolt execute:" + log);
        logs.add(log);
        System.out.println("AdLogCounterBolt execute out end");
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
        System.out.println("AdLogCounterBolt cleanup out start");
        try {
            for (String log : logs) {
                System.out.println("AdLogCounterBolt result : " + log);
                out.write((log + "\n").getBytes());
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
        System.out.println("AdLogCounterBolt cleanup out end");
    }
}