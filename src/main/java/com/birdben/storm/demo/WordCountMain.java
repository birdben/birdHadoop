package com.birdben.storm.demo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class WordCountMain {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        if (args.length != 2) {
            System.err.println("Usage: inputPath outputPath");
            System.err.println("such as : java -jar WordCount.jar /home/yunyu/Downloads/storm_inputfiles/ /home/yunyu/Downloads/storm_outputfiles/");
            System.exit(2);
        }

        // 定义一个Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-spilter", new WordSpliter()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter()).shuffleGrouping("word-spilter");
        String inputPath = args[0];
        String outputPath = args[1];

        // 设置配置属性
        Config conf = new Config();
        conf.put("INPUT_PATH", inputPath);
        conf.put("OUTPUT_PATH", outputPath);
        conf.setDebug(true);

        // 本地模式:本地提交
        // 实际上本地模式在JVM中模拟了一个Storm集群，用于开发和测试Topology。在本地模式下运行Topology类似于在集群上运行Topology。只需使用LocalCluster类就可以创建一个进程内的集群
        // 直接在IDE就可以启动Storm本地集群，可以在代码中控制集群的停止
        // 注意：需要将pom文件中的storm-core的scope修改成jar，否则IDE项目下是没有引用到此storm-core.jar包的
        // "WordCount"是Topology的名字，必须具有唯一性
        /*
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("WordCount", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("WordCount");
        cluster.shutdown();
        */

        // 集群模式:集群提交
        // 需要将代码打包成jar包，然后在Storm集群机器上运行"storm jar birdHadoop.jar com.birdben.storm.demo.WordCountMain inputpath outputpath"命令，这样该Topology会运行在不同的JVM或物理机器上，并且可以在Storm UI中监控到
        // 使用集群模式时，不能在代码中控制集群，这和LocalCluster是不一样的。无法在代码中控制集群的停止
        // 注意：需要在打包前将pom文件中的storm-core的scope修改成provided，因为在Storm集群上运行storm-core.jar已经存在了，再次引用就会冲突报错
        // "WordCount"是Topology的名字，必须具有唯一性
        StormSubmitter.submitTopology("WordCount", conf, builder.createTopology());
    }
}