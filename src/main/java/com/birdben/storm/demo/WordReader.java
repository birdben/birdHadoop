package com.birdben.storm.demo;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class WordReader extends BaseRichSpout {

    Log logger = LogFactory.getLog(WordReader.class);

    private static final long serialVersionUID = 2197521792014017918L;
    private String inputPath;
    private SpoutOutputCollector collector;

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        inputPath = (String) conf.get("INPUT_PATH");
    }

    @Override
    public void nextTuple() {
        // 这里不输出到控制台，因为没有新文件内容被读取，这句System.out会不断循环输出
        //System.out.println("WordReader nextTuple out start");
        //System.out.println("out inputPath:" + inputPath);
        Collection<File> files = FileUtils.listFiles(new File(inputPath),
                FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".bak")), null);
        Iterator it = files.iterator();
        while (it.hasNext()) {
            File f = (File) it.next();
            try {
                List<String> lines = FileUtils.readLines(f, "UTF-8");
                for (String line : lines) {
                    collector.emit(new Values(line));
                }
                FileUtils.moveFile(f, new File(f.getPath() + ".bak"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //System.out.println("WordReader nextTuple out end");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

}