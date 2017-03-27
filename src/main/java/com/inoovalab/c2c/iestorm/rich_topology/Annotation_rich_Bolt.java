package com.inoovalab.c2c.iestorm.rich_topology;

import com.inoovalab.c2c.iestorm.TweetEvent;
import gate.Annotation;
import gate.AnnotationSet;
import gate.Corpus;
import gate.Document;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by rilfi on 3/19/2017.
 */
public class Annotation_rich_Bolt extends BaseRichBolt {
    OutputCollector _collector;
    private long initiatatedTime;
    private long count;
    private long threadid;


    public Map<String, Set<String>> getAnnotatedMap(Document doc, String tweets)
             {
        String lowerTweet = tweets.toLowerCase();
        AnnotationSet obj = doc.getAnnotations();
        String[] neededList = {"Lookup"};
        String[] listinLookup = {"brand", "model", "status"};
        List<String> returnStr = new ArrayList<String>();
        Map<String, Set<String>> returnMap = new HashMap<String, Set<String>>();
        Map<String, String> modelMap = new HashMap<String, String>();
        Set<Map<String, String>> modelSet = new HashSet<>();
        for (Annotation a : obj) {
            for (String annotationType : neededList) {
                if (a.getType().contentEquals(annotationType)) {
                    if (annotationType.contains(a.getType())) {
                        for (String lul : listinLookup) {
                            Object ob = a.getFeatures();
                            if (a.getFeatures().containsValue(lul)) {
                                int startindex = a.getStartNode().getOffset().intValue();
                                int endindex = a.getEndNode().getOffset().intValue();
                                Set<String> valueSet = new HashSet<String>();
                                try {
                                    String valueStr = lowerTweet.substring(startindex, endindex);
                                    if (lul.equals("model")) {
                                        modelMap.put("product", a.getFeatures().get("minorType").toString().split("_")[1]);
                                        modelMap.put("brand", a.getFeatures().get("minorType").toString().split("_")[0]);
                                        modelMap.put("model", valueStr);
                                        modelSet.add(modelMap);

                                    }
                                    if (lul.equals("status")) {
                                        valueStr += "_" + a.getFeatures().get("minorType").toString();

                                    }
                                    valueSet.add(valueStr);
                                    if (returnMap.containsKey(lul)) {
                                        valueSet = returnMap.get(lul);
                                        valueSet.add(valueStr);
                                    }
                                    returnMap.put(lul, valueSet);

                                } catch (StringIndexOutOfBoundsException se) {
                                    System.out.println("Extraction Error");
                                    System.out.println(lowerTweet);
                                    System.out.println("annotation --" + a.toString());
                                    System.out.println("start index  " + startindex);
                                    System.out.println("Endindex  " + endindex);
                                    se.printStackTrace();

                                }

                            }

                        }
                    }

                }

            }
        }
       /* if (returnMap.get("brand") == null || returnMap.get("model") == null) {
            System.out.println("brand or model null");
            return null;
        }
        Set<String> productSet = new HashSet<>();
        if (returnMap.get("product") == null) {
            for (Map<String, String> mm : modelSet) {
                productSet.add(mm.get("product"));
            }
            returnMap.put("product", productSet);
        }
        try {
            for (Map<String, String> mm : modelSet) {
                boolean isBrand = false;
                String modelBrand = mm.get("brand");
                for (String brnd : returnMap.get("brand")) {
                    if (modelBrand.equals(brnd)) {
                        isBrand = true;
                    }
                }
                if (isBrand == false) {
                    System.out.println("isBrand null");
                    return null;
                }
            }
            Set<String> statusSet = new HashSet<>();
            String status = "";
            String statusType = "";
            *//*if (!returnMap.containsKey("status")) {
                System.out.println("status null");
                return returnMap;
            }*//*
            String state = "";
            for (String sta : returnMap.get("status")) {
                if (sta.length() > state.length()) {
                    state = sta;
                }
            }
            // status = state.split("_")[0];
            statusType = state.split("_")[1];
            statusSet.add(statusType);
            returnMap.remove("status");
            returnMap.put("status", statusSet);*/
            //return returnMap;

        final String URL_REGEX = "^((https?|ftp)://|(www|ftp)\\.)?[a-z0-9-]+(\\.[a-z0-9-]+)+([/?].*)?$";
        Pattern pattern = Pattern.compile(URL_REGEX);
        Matcher matcher = pattern.matcher(tweets);//replace with string to compare
        Set<String> urlSet = new HashSet<String>();
        if (matcher.find()) {
            urlSet.add("yes");
            returnMap.put("url", urlSet);

        } else {
            urlSet.add("no");
            returnMap.put("url", urlSet);
        }
        return returnMap;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        count = 0;
        threadid=Thread.currentThread().getId();
        initiatatedTime = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);


    }

    @Override
    public void execute(Tuple tuple) {
        TweetEvent tv=(TweetEvent)tuple.getValue(0);
        String threadIdStr=count+","+Thread.currentThread().getId()+","+threadid;
        tv.setGazetteerThreadID(threadIdStr);
        count++;
        Document doc = tv.getDocument();
        long beforeProcessTS = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);
        boolean isTerminated = false;
        try {
            Map<String,Set<String>> annotatedMap=getAnnotatedMap(tv.getDocument(),tv.getTweet());
            if(annotatedMap!=null){
                Long afterProcessTS = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);
                long averageTS = (afterProcessTS - initiatatedTime) / ++count;
                long timeTaken = afterProcessTS - beforeProcessTS;
                /*emitingMap.put("annotatedMap",annotatedMap);
                emitingMap.put("annotationTT",timeTaken);
                emitingMap.put("annotationAT",averageTS);*/
                tv.setAnnotatedMap(annotatedMap);
                tv.setAnnotationTT(timeTaken);
                tv.setAnnotationAT(averageTS);
               _collector.emit( new Values(tv));
                _collector.ack(tuple);

            }
            else {
              //  _collector.ack(tuple);
            }


        }
        catch (Exception e){
            isTerminated=true;
        }
        finally {
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
