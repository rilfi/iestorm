package com.inoovalab.c2c.iestorm;


import com.inoovalab.c2c.iestorm.basic_topology.*;
import gate.Gate;
import gate.util.GateException;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.net.MalformedURLException;

public class LocalTopologyRunner {
    public static void main(String[] args) {
        Gate.setGateHome(new File("/opt/gate-8.3-build5704-ALL"));
        try {
            Gate.init();
        } catch (GateException e) {
            e.printStackTrace();
        }
        try {
            Gate.getCreoleRegister().registerDirectories(new File(Gate.getPluginsHome(), "ANNIE").toURI().toURL());
        } catch (GateException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("Simulated_Tweet", new Simulated_Tweet_Spout(), 1);

        //builder.setBolt("Tokenizer_Bolt", new Tokenizer_Bolt(), 1).shuffleGrouping("Simulated_Tweet");
        //builder.setBolt("Gazetteer_Bolt", new Gazetteer_Bolt(), 4).shuffleGrouping("Tokenizer_Bolt");
       // builder.setBolt("Annotation_Bolt", new Annotation_Bolt(), 4).shuffleGrouping("Gazetteer_Bolt");
        builder.setBolt("Persist_Bolt", new Persist_Bolt(), 1).shuffleGrouping("Simulated_Tweet");

        Config config = new Config();
        config.setDebug(true);
        config.registerSerialization(TweetEvent.class);
        config.put("tweetFile", "tweet1.txt");
        config.put("persist.file", "output50.txt");

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("credit-card-rich_topology", config, builder.createTopology());

        Utils.sleep(600000);
        localCluster.shutdown();
    }
}