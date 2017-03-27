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
    private String tokennizerThreadID;
    private String gazetteerThreadID;
    private String annotationThreadID;

    public String getGazetteerThreadID() {
        return gazetteerThreadID;
    }

    public void setGazetteerThreadID(String gazetteerThreadID) {
        this.gazetteerThreadID = gazetteerThreadID;
    }

    public String getAnnotationThreadID() {
        return annotationThreadID;
    }

    public void setAnnotationThreadID(String annotationThreadID) {
        this.annotationThreadID = annotationThreadID;
    }


    public String getTokennizerThreadID() {
        return tokennizerThreadID;
    }

    public void setTokennizerThreadID(String tokennizerThreadID) {
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
    return   msgId +","+ started +","+ tubleStarted +"," +tokennizerThreadID+","+ tokenizerTT +
            "," + tokenizerAT +","+gazetteerThreadID+
            "," + gazetteerTT +
            "," + gazetteerAT +","+annotationThreadID+
            "," + annotationTT +
            "," + annotationAT ;
}
}
