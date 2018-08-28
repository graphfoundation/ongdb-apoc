package apoc.broker;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;

/**
 * @author alexanderiudice
 */
public interface BrokerConnection
{
    String maxPollRecordsDefault = "1";

    ObjectMapper objectMapper = new ObjectMapper(  );

    Stream<BrokerMessage> send( @Name( "message" ) Map<String,Object> message, @Name( "configuration" ) Map<String,Object> configuration ) throws Exception;

    Stream<BrokerResult> receive( @Name( "configuration" ) Map<String,Object> configuration ) throws IOException;

    void stop();

    void checkConnectionHealth() throws Exception;

    String getConnectionName();

    Log getLog();

    Map<String,Object> getConfiguration();

    Boolean isConnected();
    void setConnected( Boolean connected );

    Boolean isReconnecting();
    void setReconnecting( Boolean reconnecting );
}
