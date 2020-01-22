package apoc.broker;

import org.neo4j.logging.Log;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author alexanderiudice
 */
public class ConnectionManager
{
    private ConnectionManager()
    {
    }

    private static Map<String,BrokerConnection> brokerConnections = new ConcurrentHashMap<>();

    public static BrokerConnection addRabbitMQConnection( String connectionName, Log log, Map<String,Object> configuration )
    {
        log.info( "APOC Broker: Adding RabbitMQ Connection '" + connectionName + "' with configurations " + configuration.toString() );
        return brokerConnections.put( connectionName, RabbitMqConnectionFactory.createConnection( connectionName, log, configuration ) );
    }

    public static BrokerConnection addSQSConnection( String connectionName, Log log, Map<String,Object> configuration )
    {
        log.info( "APOC Broker: Adding SQS Connection '" + connectionName + "' with configurations " + configuration.toString() );
        return brokerConnections.put( connectionName, SqsConnectionFactory.createConnection( connectionName, log, configuration ) );
    }

    public static BrokerConnection addKafkaConnection( String connectionName, Log log, Map<String,Object> configuration )
    {
        log.info( "APOC Broker: Adding Kafka Connection '" + connectionName + "' with configurations " + configuration.toString() );
        return brokerConnections.put( connectionName, KafkaConnectionFactory.createConnection( connectionName, log, configuration ) );
    }

    public static BrokerConnection getConnection( String connectionName )
    {
        try
        {
            return brokerConnections.get( connectionName );
        }
        catch ( NullPointerException e )
        {
            throw new RuntimeException( "Tried to access non-existent connection '" + connectionName + "' in the brokerConnections map." );
        }
    }

    public static Boolean doesExist( String connectionName )
    {
        return brokerConnections.containsKey( connectionName );
    }

    public static Set<String> getConnectionNames(){
        return brokerConnections.keySet();
    }

    public static void closeConnection( String connectionName )
    {
        brokerConnections.get( connectionName ).stop();
    }

    public static void updateConnection( final String connectionName, final BrokerConnection brokerConnection )
    {
        if ( doesExist( connectionName ) )
        {
            closeConnection( connectionName );
        }

        brokerConnections.put( connectionName, brokerConnection );
    }

    public static void closeConnections()
    {
        brokerConnections.forEach( ( name, connection ) -> connection.stop() );
    }
}
