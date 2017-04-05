package com.inoovalab.c2c.iestorm;

import gate.Document;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * Created by rilfi on 3/20/2017.
 */
public class TweetEvent implements Serializable {
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

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }



    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getTweetUrl() {
        return tweetUrl;
    }

    public void setTweetUrl(String tweetUrl) {
        this.tweetUrl = tweetUrl;
    }



    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }




    public long getTcount() {
        return tcount;
    }

    public void setTcount(long tcount) {
        this.tcount = tcount;
    }

    public long getGcount() {
        return gcount;
    }

    public void setGcount(long gcount) {
        this.gcount = gcount;
    }

    public long getAcount() {
        return acount;
    }

    public void setAcount(long acount) {
        this.acount = acount;
    }


    public Map<String, String> getProductMap() {
        return productMap;
    }

    public void setProductMap(Map<String, String> productMap) {
        this.productMap = productMap;
    }


    public long getGazetteerThreadID() {
        return gazetteerThreadID;
    }

    public void setGazetteerThreadID(long gazetteerThreadID) {
        this.gazetteerThreadID = gazetteerThreadID;
    }

    public long getAnnotationThreadID() {
        return annotationThreadID;
    }

    public void setAnnotationThreadID(long annotationThreadID) {
        this.annotationThreadID = annotationThreadID;
    }


    public long getTokennizerThreadID() {
        return tokennizerThreadID;
    }

    public void setTokennizerThreadID(long tokennizerThreadID) {
        this.tokennizerThreadID = tokennizerThreadID;
    }


    public String getTweet() {
        return tweet;
    }

    public void setTweet(String tweet) {
        this.tweet = tweet;
    }

    public long getMsgId() {
        return msgId;
    }

    public void setMsgId(long msgId) {
        this.msgId = msgId;
    }

    public long getStarted() {
        return started;
    }

    public void setStarted(long started) {
        this.started = started;
    }

    public long getTubleStarted() {
        return tubleStarted;
    }

    public void setTubleStarted(long tubleStarted) {
        this.tubleStarted = tubleStarted;
    }

    public Document getDocument() {
        return document;
    }

    public void setDocument(Document document) {
        this.document = document;
    }

    public long getTokenizerTT() {
        return tokenizerTT;
    }

    public void setTokenizerTT(long tokenizerTT) {
        this.tokenizerTT = tokenizerTT;
    }

    public long getTokenizerAT() {
        return tokenizerAT;
    }

    public void setTokenizerAT(long tokenizerAT) {
        this.tokenizerAT = tokenizerAT;
    }

    public long getGazetteerTT() {
        return gazetteerTT;
    }

    public void setGazetteerTT(long gazetteerTT) {
        this.gazetteerTT = gazetteerTT;
    }

    public long getGazetteerAT() {
        return gazetteerAT;
    }

    public void setGazetteerAT(long gazetteerAT) {
        this.gazetteerAT = gazetteerAT;
    }

    public Map<String, Set<String>> getAnnotatedMap() {
        return annotatedMap;
    }

    public void setAnnotatedMap(Map<String, Set<String>> annotatedMap) {
        this.annotatedMap = annotatedMap;
    }

    public long getAnnotationTT() {
        return annotationTT;
    }


    public void setAnnotationTT(long annotationTT) {
        this.annotationTT = annotationTT;
    }

    public long getAnnotationAT() {
        return annotationAT;
    }

    public void setAnnotationAT(long annotationAT) {
        this.annotationAT = annotationAT;
    }

    /*    @Override
        public String toString() {
            return "TweetEvent{" +
                    "tweet='" + tweet + '\'' +
                    ", msgId=" + msgId +
                    ", started=" + started +
                    ", tubleStarted=" + tubleStarted +
                    //", document=" + document +
                    ", tokenizerTT=" + tokenizerTT +
                    ", tokenizerAT=" + tokenizerAT +
                    ", gazetteerTT=" + gazetteerTT +
                    ", gazetteerAT=" + gazetteerAT +
                    ", annotatedMap=" + annotatedMap +
                    ", annotationTT=" + annotationTT +
                    ", annotationAT=" + annotationAT +
                    '}';
        }*/
    @Override
    public String toString() {
        return msgId + "," + started + "," + tubleStarted + "," + tokennizerThreadID + "," + tokenizerTT +
                "," + tokenizerAT + "," + gazetteerThreadID +
                "," + gazetteerTT +
                "," + gazetteerAT + "," + annotationThreadID +
                "," + annotationTT +
                "," + annotationAT;
    }
 /*   @Override
    public String toString() {
        return   "annotationThreadID,"+annotationThreadID+",annotationTT," + annotationTT + "annotationAT ," + annotationAT ;
    }*/
}
