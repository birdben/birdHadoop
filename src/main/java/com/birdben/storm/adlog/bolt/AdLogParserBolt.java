package com.birdben.storm.adlog.bolt;

import java.util.List;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.birdben.mapreduce.adlog.parser.AdLogParser;

public class AdLogParserBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        System.out.println("AdLogParserBolt execute out start");
        String logString = input.getString(0);
        //System.out.println("AdParserBolt execute:" + str);
        try {
            List<String> logs = AdLogParser.convertLogToAd(logString);
            for (String log : logs) {
                collector.emit(new Values(log));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("AdLogParserBolt execute out end");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("logs"));
    }
}