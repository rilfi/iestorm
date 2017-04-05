package com.inoovalab.c2c.iestorm.rich_topology;

import com.inoovalab.c2c.iestorm.TweetEvent;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by a1 on 4/2/2017.
 */
public class EventValidator_rich_Bolt extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        TweetEvent tv = (TweetEvent) tuple.getValue(0);
        boolean isValid = false;
        Map<String, Set<String>> annotatedMap = tv.getAnnotatedMap();
        Set<String> brandSet = tv.getAnnotatedMap().get("brand");
        Set<String> statusSet = tv.getAnnotatedMap().get("status");
        Set<String> modelSet = tv.getAnnotatedMap().get("model");
        Set<String> productSet = new HashSet<>();
        for (String modelLine : modelSet) {
            Map<String, String> productMap = new HashMap<>();
            String model = modelLine.split("_")[0];
            String brand = modelLine.split("_")[1];
            String product = modelLine.split("_")[2];
            if (brandSet.contains(brand)) {
                productMap.put("brand", brand);
                productMap.put("product", product);
                productMap.put("model",model);
                String state = "";
                if(statusSet.contains("sell")){
                    state="sell";

                }
                else {
                    for (String sta : annotatedMap.get("status")) {
                        if (sta.length() > state.length()) {
                            state = sta;
                        }
                    }
                    state=state.split("_")[1];
                }
                productMap.put("status",state);
                tv.setProductMap(productMap);
                _collector.emit(new Values(tv));


            }



        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("emitingMap"));

    }
}
