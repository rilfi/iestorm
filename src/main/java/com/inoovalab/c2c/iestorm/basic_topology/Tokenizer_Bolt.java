package com.inoovalab.c2c.iestorm.basic_topology;

import com.inoovalab.c2c.iestorm.TweetEvent;
import gate.Corpus;
import gate.Document;
import gate.Factory;
import gate.LanguageAnalyser;
import gate.creole.ExecutionException;
import gate.creole.ResourceInstantiationException;
import gate.creole.SerialAnalyserController;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by rilfi on 3/19/2017.
 */
public class Tokenizer_Bolt extends BaseBasicBolt {

    SerialAnalyserController tokenizerPR;
    private long initiatatedTime;
    private long count;

    private SerialAnalyserController loadController() {
        SerialAnalyserController annieController = null;
        try {
            annieController =
                    (SerialAnalyserController) Factory.createResource(
                            "gate.creole.SerialAnalyserController");
        } catch (ResourceInstantiationException e) {
            e.printStackTrace();
        }
        LanguageAnalyser tokenpr = null;
        try {
            tokenpr = (LanguageAnalyser)
                    Factory.createResource(
                            "gate.creole.tokeniser.DefaultTokeniser");
        } catch (ResourceInstantiationException e) {
            e.printStackTrace();
        }

        annieController.add(tokenpr);
        return annieController;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

        count=0;
        ThreadLocal<SerialAnalyserController> controller = new ThreadLocal<SerialAnalyserController>() {

            protected SerialAnalyserController initialValue() {
                return loadController();
            }

        };
        tokenizerPR = controller.get();
        initiatatedTime = System.nanoTime()-(24*60*60*1000*1000*1000);


    }



        @Override
        public void declareOutputFields (OutputFieldsDeclarer outputFieldsDeclarer){
            outputFieldsDeclarer.declare(new Fields("emitingMap"));
        }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        System.out.println("**************"+tuple.toString());
        TweetEvent tv=(TweetEvent)tuple.getValue(0) ;

        // Map<String,Object>emitingMap=(Map<String, Object>) tuple.getValue(0);
        //System.out.println("---#############------"+emitingMap.keySet());
        // String tweet = emitingMap.get("tweet").toString();
        Corpus corpus = null;
        long beforeProcessTS = System.nanoTime()-(24*60*60*1000*1000*1000);
        boolean isTerminated=false;
        try {
            corpus = Factory.newCorpus("SingleTweetCorpus");

            Document doc = null;

            doc = Factory.newDocument(tv.getTweet());

            corpus.add(doc);
            tokenizerPR.setCorpus(corpus);

            tokenizerPR.execute();


            Long afterProcessTS = System.nanoTime()-(24*60*60*1000*1000*1000);
            long averageTS=(afterProcessTS-initiatatedTime)/++count;
            long timeTaken = afterProcessTS - beforeProcessTS;
            /*emitingMap.put("document",doc);
            emitingMap.put("TokenizerTT",timeTaken);
            emitingMap.put("TokenizerAT",averageTS);*/
            tv.setDocument(doc);
            tv.setTokenizerTT(timeTaken);
            tv.setTokenizerAT(averageTS);
            basicOutputCollector.emit( new Values(tv));
            Factory.deleteResource(doc);
            Factory.deleteResource(corpus);
            //Factory.deleteResource(tokenizerPR);
            // _collector.ack(tuple);
            // corpus.clear();
            // doc.cleanup();

        } catch (ExecutionException e) {
            e.printStackTrace();
            isTerminated=true;
        } catch (ResourceInstantiationException e) {
            e.printStackTrace();
            isTerminated=true;
        }
        finally {
            if(isTerminated){
                // _collector.ack(tuple);
            }

        }

    }
}
