package experiment;

/**
 * Created by a1 on 4/4/2017.
 */
import org.glassfish.jersey.SslConfigurator;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
public class DasDataManipulate {
    WebTarget webTargetCount;
    SslConfigurator sslConfig ;
    SSLContext sslContext ;
    //HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic("Basic", "YWRtaW46YWRtaW4=");
    HttpAuthenticationFeature feature ;
    Client client ;
    WebTarget webTarget;
    WebTarget webTargetSc;

    public DasDataManipulate() {
        sslConfig = SslConfigurator.newInstance()
                .trustStoreFile("client-truststore.jks")
                .trustStorePassword("wso2carbon")
                .keyStoreFile("wso2carbon.jks")
                .keyPassword("wso2carbon");
        sslContext = sslConfig.createSSLContext();
        //HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic("Basic", "YWRtaW46YWRtaW4=");
        feature = HttpAuthenticationFeature.basic("admin", "admin");
        client = ClientBuilder.newBuilder().sslContext(sslContext).build();
        webTarget = client.target("https://localhost:9443").path("analytics/search").register(feature);
        webTargetSc=client.target("https://localhost:9443").path("analytics/search_count").register(feature);
        webTargetCount=client.target("https://localhost:9443").path("analytics/tables/TES").register(feature);

    }
  /*  public  String getRealtedRecordes(String brand,int relatedCount){

        String payload = "{\"tableName\":\"TES\",\"query\":\"idd:"+brand+"\",\"start\":0,\"count\":"+relatedCount+"}";
        Response response = webTarget.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(payload));
        return response.readEntity(String.class);

    }*/
    public  String getRealted(){

       // String payload = "{\"tableName\":\"TES\",\"query\":\"idd:"+brand+"\",\"start\":0,\"count\":"+relatedCount+"}";
        Response response = webTargetCount.request().get();
        return response.readEntity(String.class);

    }

/*    public  String getRealtedCount(String searchWord){
        //'{"tableName":"ORG_W", "query":"state:Texas", "start":0, "count":3}'
        String payload = "{\"tableName\":\"TES\",\"query\":\"idd:"+searchWord+"\",\"start\":0, \"count\":3}";
        Response response = webTargetSc.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(payload));
        *//*System.out.println(response.getStatus());
        System.out.println(response.readEntity(String.class));
        System.out.println(response);*//*
        return response.readEntity(String.class);

    }*/

    public static void main(String[] args) {
        DasDataManipulate dasDataManipulate=new DasDataManipulate();
        String countstr = dasDataManipulate.getRealted();
        System.out.println(countstr);

       /* int relatedCount = 0;
        try {

            relatedCount = Integer.parseInt(countstr);
        } catch (NullPointerException ne) {
            System.out.println("nul  count");
        }
        if (relatedCount > 0) {

            String jsonArray = dasDataManipulate.getRealtedRecordes("one", relatedCount);
            System.out.println("json array----" + jsonArray);
            System.out.println("  --count " + relatedCount);

        }*/
    }

}
