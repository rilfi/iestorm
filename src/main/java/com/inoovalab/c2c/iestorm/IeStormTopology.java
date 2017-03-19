
package com.inoovalab.c2c.iestorm;

import com.inoovalab.c2c.iestorm.topology.*;
import gate.Gate;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.util.Map;

public class IeStormTopology {






    public static void printMetrics(Nimbus.Client client, String name) throws Exception {
        ClusterSummary summary = client.getClusterInfo();
        String id = null;
        for (TopologySummary ts: summary.get_topologies()) {
            if (name.equals(ts.get_name())) {
                id = ts.get_id();
            }
        }
        if (id == null) {
            throw new Exception("Could not find a topology named "+name);
        }
        TopologyInfo info = client.getTopologyInfo(id);
        int uptime = info.get_uptime_secs();
        long acked = 0;
        long failed = 0;
        double weightedAvgTotal = 0.0;
        for (ExecutorSummary exec: info.get_executors()) {
            if ("spout".equals(exec.get_component_id())) {
                SpoutStats stats = exec.get_stats().get_specific().get_spout();
                Map<String, Long> failedMap = stats.get_failed().get(":all-time");
                Map<String, Long> ackedMap = stats.get_acked().get(":all-time");
                Map<String, Double> avgLatMap = stats.get_complete_ms_avg().get(":all-time");
                for (String key: ackedMap.keySet()) {
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
        double avgLatency = weightedAvgTotal/acked;
        System.out.println("uptime: "+uptime+" acked: "+acked+" avgLatency: "+avgLatency+" acked/sec: "+(((double)acked)/uptime+" failed: "+failed));
    }

    public static void kill(Nimbus.Client client, String name) throws Exception {
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);
        client.killTopologyWithOpts(name, opts);
    }

    public static void main(String[] args) throws Exception {
        Gate.setGateHome(new File("/opt/gate-8.3-build5704-ALL"));
        Gate.init();
        Gate.getCreoleRegister().registerDirectories(new File(Gate.getPluginsHome(), "ANNIE").toURI().toURL());


        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("Simulated_Tweet", new Simulated_TweetSpout(), 1);

        builder.setBolt("TokenizerBolt", new TokenizerBolt(), 4).shuffleGrouping("Simulated_Tweet");
        builder.setBolt("GazetteerBolt", new GazetteerBolt(), 4).shuffleGrouping("TokenizerBolt");
        builder.setBolt("AnnotationBolt", new AnnotationBolt(), 4).shuffleGrouping("GazetteerBolt");
        builder.setBolt("PersistBolt", new PersistBolt(), 4).shuffleGrouping("AnnotationBolt");

        Config conf = new Config();
        conf.registerMetricsConsumer(org.apache.storm.metric.LoggingMetricsConsumer.class);
        conf.put("tweetFile", "tweet50.txt");
        conf.put("persist.file", "output50.txt");


        String name = "wc-test";
        if (args != null && args.length > 0) {
            name = args[0];
        }

        conf.setNumWorkers(1);
        StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());

        Map clusterConf = Utils.readStormConfig();
        clusterConf.putAll(Utils.readCommandLineOpts());
        Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

        //Sleep for 5 mins
        for (int i = 0; i < 10; i++) {
            Thread.sleep(30 * 1000);
            printMetrics(client, name);
        }
        kill(client, name);
    }
}