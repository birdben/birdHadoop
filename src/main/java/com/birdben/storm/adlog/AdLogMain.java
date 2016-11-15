package com.birdben.storm.adlog;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.birdben.storm.adlog.bolt.AdLogCounterBolt;
import com.birdben.storm.adlog.bolt.AdLogParserBolt;
import com.birdben.storm.adlog.spout.AdLogReaderSpout;

public class AdLogMain {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        if (args.length != 2) {
            System.err.println("Usage: inputPath outputPath");
            System.err.println("such as : java -jar birdHadoop.jar /home/yunyu/Downloads/storm_inputfiles/ /home/yunyu/Downloads/storm_outputfiles/");
            System.exit(2);
        }

        // 定义一个Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("adlog-reader", new AdLogReaderSpout());
        builder.setBolt("adlog-parser", new AdLogParserBolt()).shuffleGrouping("adlog-reader");
        builder.setBolt("adlog-counter", new AdLogCounterBolt()).shuffleGrouping("adlog-parser");
        String inputPath = args[0];
        String outputPath = args[1];

        // 设置配置属性
        Config conf = new Config();
        conf.put("INPUT_PATH", inputPath);
        conf.put("OUTPUT_PATH", outputPath);
        conf.setDebug(true);

        // 本地模式:本地提交
        /*
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("AdLog", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("AdLog");
        cluster.shutdown();
        */

        // 集群模式:集群提交
        StormSubmitter.submitTopology("AdLog", conf, builder.createTopology());
    }
}
