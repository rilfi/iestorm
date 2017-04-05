package experiment;

import com.inoovalab.c2c.iestorm.rich_topology.RT_Das_rich_Bolt;
import org.wso2.carbon.databridge.agent.AgentHolder;
import org.wso2.carbon.databridge.agent.DataPublisher;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAgentConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAuthenticationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointException;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.carbon.databridge.commons.exception.TransportException;

/**
 * Created by a1 on 4/1/2017.
 */
public class DasDataSent {
    DataPublisher dataPublisher;
    String protocol;
    String host;
    String port;
    String username;
    String password;
    String streamId;

    public DasDataSent() {
        ClassLoader classLoader = getClass().getClassLoader();
        //System.out.println("src/main/resources/data-agent-config.xml");
        AgentHolder. setConfigPath ("src/main/resources/data-agent-config.xml");
        DataPublisherUtil.setTrustStoreParams();
        //System.out.println(String.valueOf(DasDataSent.class.getClassLoader().getResource("data-agent-conf.xml")));

        this.protocol = "thrift";
        this.host = "192.248.8.248";
        this.port = "7611";
        this.username = "admin";
        this.password = "admin";
        this.streamId = "test:1.0.0";
        try {
            //dataPublisher = new DataPublisher( "tcp://" + host + ":" + port, username, password);
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

    public static void main(String[] args) {
        DasDataSent dasDataSent=new DasDataSent();
        Event event = new Event(dasDataSent.streamId, System.currentTimeMillis(), null, null, new Object[]{11});
        dasDataSent.dataPublisher.publish(event);
    }
}
