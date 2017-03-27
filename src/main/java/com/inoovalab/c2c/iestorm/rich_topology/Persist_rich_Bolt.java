package com.inoovalab.c2c.iestorm.rich_topology;

import com.inoovalab.c2c.iestorm.TweetEvent;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Created by rilfi on 3/19/2017.
 */
public class Persist_rich_Bolt extends BaseRichBolt {
    OutputCollector _collector;
    private BufferedWriter writer;
    boolean isTerminated;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        String filepath = (String) map.get("persist.file");
        String absoluteFileName = filepath + "." + topologyContext.getThisTaskIndex();
        this._collector = outputCollector;
        isTerminated=false;
        try {
            writer = new BufferedWriter(new FileWriter(absoluteFileName));
            String head="tweet,msgId,started,tubleStarted,tokennizerThreadID,tokenizerTT,tokenizerAT,gazetteerThreadID,gazetteerTT"
                      +",gazetteerAT,annotationThreadID,annotatedMap,annotationTT,annotationAT";
            writer.write(head);
            writer.newLine();
        } catch (IOException e) {
            throw new RuntimeException("Problem opening file " + absoluteFileName, e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        TweetEvent tv = (TweetEvent) tuple.getValue(0);
        try {

            writer.write(tv.toString());
            writer.newLine();

            writer.flush();
           _collector.ack(tuple);
        } catch (IOException e) {
            e.printStackTrace();
        }
        catch (Exception e){
            e.printStackTrace();
            isTerminated=true;

        }
        finally {
            if(isTerminated){
                _collector.fail(tuple);
            }
        }





    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

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

