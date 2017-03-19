package com.inoovalab.c2c.iestorm.topology;


import org.apache.commons.collections.map.HashedMap;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import java.util.Map;

/**
 * Created by rilfi on 3/19/2017.
 */
public class Simulated_TweetSpout extends BaseRichSpout {
    private static final Logger LOGGER = LogManager.getLogger(Simulated_TweetSpout.class);
    private long msgId = 0;
    private String fileName;
    private long started;
    private SpoutOutputCollector outputCollector;



    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector = spoutOutputCollector;
        this.fileName = (String) map.get("tweetFile");
        started=System.nanoTime()-(24*60*60*1000*1000*1000);

    }

    @Override
    public void nextTuple() {
        Map<String,Object>emitingMap= new HashedMap();
        try {
            Path filePath = Paths.get(this.getClass().getClassLoader().getResource(fileName).toURI());
            try (Stream<String> tweets = Files.lines(filePath)) {
                tweets.forEach(tweet -> {
                    emitingMap.put("tweet",tweet);
                    emitingMap.put("started",started);
                    emitingMap.put("tubleStarted",System.nanoTime()-(24*60*60*1000*1000*1000));
                    emitingMap.put("msgId",++msgId);

                    outputCollector.emit(new Values(emitingMap),msgId);
                    emitingMap.clear();
                });
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
            }
        } catch (URISyntaxException e) {
            LOGGER.error(e.getMessage());
        }

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
