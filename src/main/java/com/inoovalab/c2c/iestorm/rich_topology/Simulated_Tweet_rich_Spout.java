package com.inoovalab.c2c.iestorm.rich_topology;


import java.nio.charset.StandardCharsets;
import java.util.*;

import com.inoovalab.c2c.iestorm.TweetEvent;
import com.inoovalab.c2c.iestorm.basic_topology.Simulated_Tweet_Spout;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Created by rilfi on 3/19/2017.
 */
public class Simulated_Tweet_rich_Spout extends BaseRichSpout {
   private static final Logger LOGGER = LogManager.getLogger(Simulated_Tweet_Spout.class);
    private long msgId = 0;
    private String fileName;
    private long started;
    private SpoutOutputCollector outputCollector;



    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector = spoutOutputCollector;
        this.fileName = (String) map.get("tweetFile");
        started=System.nanoTime()-(24*60*60*1000*1000*1000);
        System.out.println(fileName);

    }

    @Override
    public void nextTuple() {
       // Map<String,Object>emitingMap= new HashMap<>();
        try {
            List<String> tweets = Files.readAllLines(Paths.get(fileName), StandardCharsets.UTF_8);
            for(String tweet:tweets) {


               /* TweetEvent tv = new TweetEvent();
                tv.setTweet(tweet);
                tv.setStarted(started);
                tv.setTubleStarted(System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000));
                tv.setMsgId(msgId);*/
                outputCollector.emit(new Values(tweet));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
/*        Path filePath = Paths.get(fileName);
        System.out.println(filePath.toAbsolutePath().toString());
        try (Stream<String> tweets = Files.lines(filePath)) {
            tweets.forEach(tweet -> {

                *//*emitingMap.put("tweet",tweet);
                emitingMap.put("started",started);
                emitingMap.put("tubleStarted",System.nanoTime()-(24*60*60*1000*1000*1000));
                emitingMap.put("msgId",++msgId);*//*
                TweetEvent tv=new TweetEvent();
                tv.setTweet(tweet);
                tv.setStarted(started);
                tv.setTubleStarted(System.nanoTime()-(24*60*60*1000*1000*1000));
                tv.setMsgId(msgId);

                outputCollector.emit(new Values(tv),msgId);

               // emitingMap.clear();
            });
        } catch (IOException e) {
            //LOGGER.error(e.getMessage());
        }*/

    }

    @Override
    public void ack(Object msgId) {
        LOGGER.debug("Got ACK for msgId : " + msgId);
    }

    @Override
    public void fail(Object msgId) {
        LOGGER.debug("Got FAIL for msgId : " + msgId);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("emitingMap"));

    }
}
