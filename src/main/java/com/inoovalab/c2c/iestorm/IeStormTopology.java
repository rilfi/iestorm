package com.inoovalab.c2c.iestorm;

import com.inoovalab.c2c.iestorm.rich_topology.*;
import gate.Gate;
import gate.util.GateException;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.Map;

public class IeStormTopology implements Serializable {


    public static void printMetrics(Nimbus.Client client, String name) throws Exception {
        ClusterSummary summary = client.getClusterInfo();
        String id = null;
        for (TopologySummary ts : summary.get_topologies()) {
            if (name.equals(ts.get_name())) {
                id = ts.get_id();
            }
        }
        if (id == null) {
            throw new Exception("Could not find a rich_topology named " + name);
        }
        TopologyInfo info = client.getTopologyInfo(id);
        int uptime = info.get_uptime_secs();
        long acked = 0;
        long failed = 0;
        double weightedAvgTotal = 0.0;
        for (ExecutorSummary exec : info.get_executors()) {
            if ("spout".equals(exec.get_component_id())) {
                SpoutStats stats = exec.get_stats().get_specific().get_spout();
                Map<String, Long> failedMap = stats.get_failed().get(":all-time");
                Map<String, Long> ackedMap = stats.get_acked().get(":all-time");
                Map<String, Double> avgLatMap = stats.get_complete_ms_avg().get(":all-time");
                for (String key : ackedMap.keySet()) {
                    if (failedMap != null) {
                        Long tmp = failedMap.get(key);
                        if (tmp != null) {
                            failed += tmp;
                        }
                    }
                    long ackVal = ackedMap.get(key);
                    double latVal = avgLatMap.get(key) * ackVal;
                    acked += ackVal;
                    weightedAvgTotal += latVal;
                }
            }
        }
        double avgLatency = weightedAvgTotal / acked;
        System.out.println("uptime: " + uptime + " acked: " + acked + " avgLatency: " + avgLatency + " acked/sec: " + (((double) acked) / uptime + " failed: " + failed));
    }

    public static void kill(Nimbus.Client client, String name) throws Exception {
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);
        client.killTopologyWithOpts(name, opts);
    }

    public static void main(String[] args) throws Exception {

        /*try {
            Gate.setGateHome(new File("/opt/gate-8.3-build5704-ALL"));
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
        }*/


        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("Simulated_Tweet", new Simulated_Tweet_rich_Spout(), 1);

        builder.setBolt("Tokenizer_Bolt", new Tokenizer_rich_Bolt(), 1).shuffleGrouping("Simulated_Tweet");
        builder.setBolt("Gazetteer_Bolt", new Gazetteer_rich_Bolt(), 1).shuffleGrouping("Tokenizer_Bolt");
        builder.setBolt("Annotation_Bolt", new Annotation_rich_Bolt(), 1).shuffleGrouping("Gazetteer_Bolt");
        builder.setBolt("Persist_Bolt", new Persist_rich_Bolt(), 1).shuffleGrouping("Annotation_Bolt");

        Config conf = new Config();
        conf.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class);
        conf.registerSerialization(TweetEvent.class);
        // conf.put("tweetFile", "tweet50.txt");
        //conf.setDebug(true);
        conf.put("persist.file", "output100.txt");


        String name = "IEStorm";
        if (args != null && args.length > 0) {
            name = args[0];
        }

        conf.setNumWorkers(1);
       // StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(name, conf, builder.createTopology());

        Utils.sleep(100000);
        localCluster.shutdown();


        /*Map clusterConf = Utils.readStormConfig();
        clusterConf.putAll(Utils.readCommandLineOpts());
        Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();




        //Sleep for 5 mins
        for (int i = 0; i < 5; i++) {
            Thread.sleep(30 * 1000);
            printMetrics(client, name);
        }
        kill(client, name);
        localCluster.shutdown();*/
    }
}