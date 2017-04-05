package com.inoovalab.c2c.iestorm.rich_topology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.glassfish.jersey.SslConfigurator;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Map;

/**
 * Created by rilfi on 3/19/2017.
 */
public class Related_Das_rich_Bolt extends BaseRichBolt {
    SslConfigurator sslConfig ;
    SSLContext sslContext ;
    //HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic("Basic", "YWRtaW46YWRtaW4=");
    HttpAuthenticationFeature feature ;
    Client client ;
    WebTarget webTarget;
    WebTarget webTargetSc;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        sslConfig = SslConfigurator.newInstance()
                .trustStoreFile("/home/rilfi/C2C_Real-time_Matching/src/main/resources/client-truststore.jks")
                .trustStorePassword("wso2carbon")
                .keyStoreFile("/home/rilfi/C2C_Real-time_Matching/src/main/resources/wso2carbon.jks")
                .keyPassword("wso2carbon");
        sslContext = sslConfig.createSSLContext();
        //HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic("Basic", "YWRtaW46YWRtaW4=");
        feature = HttpAuthenticationFeature.basic("admin", "admin");
        client = ClientBuilder.newBuilder().sslContext(sslContext).build();
        webTarget = client.target("https://localhost:9443").path("analytics/search").register(feature);
        webTargetSc=client.target("https://localhost:9443").path("analytics/search_count").register(feature);

    }
    public  String getRealtedRecordes(String brand,String product,String model, String status,int relatedCount){

        String payload = "{\"tableName\":\"INPUTSTREAMTOPERSIST\",\"query\":\"brand:"+brand+"\",\"product:" + product + "\",\"model:" + model + "\",\"status:" + status + "\",\"start\":0,\"count\":"+relatedCount+"}";
        Response response = webTarget.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(payload));
        return response.readEntity(String.class);

    }
    public  String getRealtedCount(String searchWord){
        //'{"tableName":"ORG_W", "query":"state:Texas", "start":0, "count":3}'
        String payload = "{\"tableName\":\"INPUTSTREAMTOPERSIST\",\"query\":\"brand:"+searchWord+"\",\"start\":0, \"count\":3}";
        Response response = webTargetSc.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(payload));
        /*System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
        System.out.println(response);*/
        return response.readEntity(String.class);

    }

    @Override
    public void execute(Tuple tuple) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
