package com.inoovalab.c2c.iestorm.rich_topology;

import com.inoovalab.c2c.iestorm.TweetEvent;
import experiment.DataPublisherUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.wso2.carbon.databridge.agent.AgentHolder;
import org.wso2.carbon.databridge.agent.DataPublisher;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAgentConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAuthenticationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointException;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.carbon.databridge.commons.exception.TransportException;

import java.util.Map;

/**
 * Created by rilfi on 3/19/2017.
 */
public class RT_Das_rich_Bolt extends BaseRichBolt {

    DataPublisher dataPublisher;
    String protocol;
    String host;
    String port;
    String username;
    String password;
    String streamId;
    String streamId1;
    String sampleNumber;
    int delay;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        AgentHolder. setConfigPath ("src/main/resources/data-agent-config.xml");
        DataPublisherUtil.setTrustStoreParams();
        //dataPublisher =  new  DataPublisher(url, username, password);
        protocol = "thrift";
        host = "localhost";
        port = "7611";
        username = "admin";
        password = "admin";
        streamId = "InputStream:1.0.0";
        streamId1 = "RelatedStream:1.0.0";
        sampleNumber = "0007";
        try {
            dataPublisher = new DataPublisher(protocol, "tcp://" + host + ":" + port, null, username, password);
        } catch (DataEndpointAgentConfigurationException e) {
            e.printStackTrace();
        } catch (DataEndpointException e) {
            e.printStackTrace();
        } catch (DataEndpointConfigurationException e) {
            e.printStackTrace();
        } catch (DataEndpointAuthenticationException e) {
            e.printStackTrace();
        } catch (TransportException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple tuple) {
        TweetEvent tv=(TweetEvent)tuple.getValue(0);
/*
        Map<String, Set<String>> annotatedMap;
        private String tweet;
        private long msgId;
        private long started;
        private long tubleStarted;
        private Document document;
        private long tokenizerTT;
        private long tokenizerAT;
        private long gazetteerTT;
        private long gazetteerAT;
        private long annotationTT;
        private long annotationAT;
        private long gcount;
        private long acount;
        private long tcount;
        private long tokennizerThreadID;
        private long gazetteerThreadID;
        private long annotationThreadID;
        private Map<String, String> productMap;
        private String brand;
        private String product;
        private String model;
        private String status;
        private String userName;
        private String tweetUrl;
        private String location;

        */
        Object metaDataArray[]={tv.getTweet(),tv.getTokennizerThreadID(),tv.getGazetteerThreadID(),tv.getAnnotationThreadID(),tv.getUserName(),tv.getTweetUrl(),tv.getLocation()};
        Object correlationdataArray[]={tv.getStarted(),tv.getTubleStarted(),tv.getTokenizerTT(),tv.getTokenizerAT(),tv.getGazetteerTT(),tv.getGazetteerAT(),tv.getAnnotationTT(),tv.getAnnotationAT(),tv.getTcount(),tv.getGcount(),tv.getAcount()};
        Object payloadDataArray[]={tv.getMsgId(),tv.getBrand(),tv.getProduct(),tv.getModel(),tv.getStatus()};





        Event event = new Event(streamId, System.currentTimeMillis(), metaDataArray, correlationdataArray, payloadDataArray);
       dataPublisher.publish(event);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
