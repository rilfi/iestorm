package com.inoovalab.c2c.iestorm.topology;

import gate.Corpus;
import gate.Document;
import gate.Factory;
import gate.LanguageAnalyser;
import gate.creole.ExecutionException;
import gate.creole.ResourceInstantiationException;
import gate.creole.SerialAnalyserController;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Set;

/**
 * Created by rilfi on 3/19/2017.
 */
public class GazetteerBolt extends BaseRichBolt {
    OutputCollector _collector;
    SerialAnalyserController gazetteerPR;
    Corpus corpus;
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
        LanguageAnalyser gazetteerpr = null;
        try {
            gazetteerpr = (LanguageAnalyser)
                    Factory.createResource(
                            "gate.creole.gazetteer.DefaultGazetteer");
        } catch (ResourceInstantiationException e) {
            e.printStackTrace();
        }
        annieController.add(gazetteerpr);

        return annieController;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        count = 0;
        ThreadLocal<SerialAnalyserController> controller = new ThreadLocal<SerialAnalyserController>() {

            protected SerialAnalyserController initialValue() {
                return loadController();
            }

        };
        try {
            corpus = Factory.newCorpus("gazetteerCorpus");
        } catch (ResourceInstantiationException e) {
            e.printStackTrace();
        }
        gazetteerPR = controller.get();
        initiatatedTime = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);


    }

    @Override
    public void execute(Tuple tuple) {
        Map<String,Object>emitingMap=(Map<String, Object>) tuple.getValue(0);
        Document doc = (Document)emitingMap.get("document");

        long beforeProcessTS = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);
        boolean isTerminated = false;
        try {


            corpus.add(doc);
            gazetteerPR.setCorpus(corpus);

            gazetteerPR.execute();
            emitingMap.remove("document");
            Long afterProcessTS = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);
            long averageTS = (afterProcessTS - initiatatedTime) / ++count;
            long timeTaken = afterProcessTS - beforeProcessTS;
            emitingMap.put("document",doc);
            emitingMap.put("gazetteerTT",timeTaken);
            emitingMap.put("gazetteerAT",averageTS);
            _collector.emit(tuple, new Values(emitingMap));
            _collector.ack(tuple);
            corpus.clear();
            doc.cleanup();
            _collector.emit(tuple, new Values());
            _collector.ack(tuple);

        } catch (ExecutionException e) {
            e.printStackTrace();
            isTerminated = true;
        } finally {
            if (isTerminated) {
                _collector.fail(tuple);
            }

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("emitingMap"));
    }
}
