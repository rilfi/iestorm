package com.inoovalab.c2c.iestorm.basic_topology;

import com.inoovalab.c2c.iestorm.TweetEvent;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Created by rilfi on 3/19/2017.
 */
public class Persist_Bolt extends BaseBasicBolt {
    private BufferedWriter writer;

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        String filepath = (String) map.get("persist.file");
        String absoluteFileName = filepath + "." + topologyContext.getThisTaskIndex();
        try {
            writer = new BufferedWriter(new FileWriter(absoluteFileName));
        } catch (IOException e) {
            throw new RuntimeException("Problem opening file " + absoluteFileName, e);
        }
    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        TweetEvent tv = (TweetEvent) tuple.getValue(0);
        //String tweet=(String)tuple.getValue(0);
        try {

            writer.write(tv.toString());
            writer.newLine();

            writer.flush();
            // _collector.ack(tuple);
        } catch (IOException e) {
            e.printStackTrace();

            // _collector.ack(tuple);
        }

    }

    @Override
    public void cleanup() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

