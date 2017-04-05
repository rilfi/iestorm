package com.inoovalab.c2c.iestorm.rich_topology;

import com.inoovalab.c2c.iestorm.TweetEvent;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by a1 on 4/2/2017.
 */
public class Filtering_rich_Bolt extends BaseRichBolt {
    OutputCollector _collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        TweetEvent tv=(TweetEvent)tuple.getValue(0);
        Map<String,Set<String>> annotatedMap=tv.getAnnotatedMap();
        if(annotatedMap.containsKey("brand")&&annotatedMap.containsKey("model")){
            if(!annotatedMap.containsKey("status")&&annotatedMap.get("url").equals("yes"))
            {
                Set<String> statusSet=new HashSet<>();
                statusSet.add("sell");
                annotatedMap.put("status",statusSet);
                tv.setAnnotatedMap(annotatedMap);

            }
            else if(annotatedMap.containsKey("status")){
                _collector.emit( new Values(tv));


            }
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("emitingMap"));

    }
}
