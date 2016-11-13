package com.birdben.storm.demo;

import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class WordSpliter extends BaseBasicBolt {

    private static final long serialVersionUID = -5653803832498574866L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        System.out.println("WordSpliter execute out start");
        String line = input.getString(0);
        String[] words = line.split(" ");
        for (String word : words) {
            word = word.trim();
            if (StringUtils.isNotBlank(word)) {
                word = word.toLowerCase();
                collector.emit(new Values(word));
            }
        }
        System.out.println("WordSpliter execute out end");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

}