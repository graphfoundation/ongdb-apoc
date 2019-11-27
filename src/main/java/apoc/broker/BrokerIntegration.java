package apoc.broker;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.broker.logging.BrokerLogManager;
import apoc.broker.logging.BrokerLogger;
import apoc.result.MapResult;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.broker.ConnectionManager.doesExist;
import static apoc.broker.ConnectionManager.getConnection;

/**
 *
 * Integrates the various broker pieces together.
 *
 * @author alexanderiudice
 * @since 2018.09
 */
public class BrokerIntegration
{

    @Context
    public static Pools pools;

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.send(connectionName, message, configuration) - Send a message to the broker associated with the connectionName namespace. Takes in parameter which are dependent on the broker being used." )
    public Stream<BrokerMessage> send( @Name( "connectionName" ) String connectionName, @Name( "message" ) Map<String,Object> message,
            @Name( "configuration" ) Map<String,Object> configuration ) throws Exception
    {

        return BrokerHandler.sendMessageToBrokerConnection( connectionName, message, configuration );
    }

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.receive(connectionName, configuration) - Receive a message from the broker associated with the connectionName namespace. Takes in a configuration map which is dependent on the broker being used." )
    public Stream<BrokerResult> receive( @Name( "connectionName" ) String connectionName, @Name( "configuration" ) Map<String,Object> configuration )
            throws IOException
    {

        return BrokerHandler.receiveMessageFromBrokerConnection( connectionName, configuration );
    }

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.flipConnection(connectionName) - A method used for flipping the connection between on and off. For testing purposes." )
    public Stream<MapResult> flipConnection( @Name( "connectionName" ) String connectionName )
    {

        return BrokerHandler.flipConnection( connectionName );
    }

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.flipReconnect(connectionName) - A method used for flipping the reconnect between on and off. Turn it to TRUE to stop reconnect from happening. For testing purposes." )
    public Stream<MapResult> flipReconnect( @Name( "connectionName" ) String connectionName )
    {

        return BrokerHandler.flipReconnect( connectionName );
    }

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.checkConnection(connectionName) - A method used for checking the connection of a specified namespace. For testing purposes." )
    public Stream<MapResult> checkConnection( @Name( "connectionName" ) String connectionName )
    {

        return BrokerHandler.checkConnection( connectionName );
    }

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.checkReconnect(connectionName) - A method used for checking the reconnection status of a specified namespace. For testing purposes." )
    public Stream<MapResult> checkReconnect( @Name( "connectionName" ) String connectionName )
    {

        return BrokerHandler.checkReconnect( connectionName );
    }


    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.resendMessages(connectionName) - A method used that resends the messages for the specified connection." +
            " Messages are retrieved from the logging file associated with that connection. Broker logging must be enabled or this method will throw an error. For testing purposes." )
    public Stream<MapResult> resendMessages( @Name( "connectionName" ) String connectionName, @Name(value = "numToSend", defaultValue = "0") Long numToSend )
    {
        BrokerHandler.retryMessagesForConnectionAsync( connectionName, numToSend );
        Map<String,Object> result = new HashMap<>(  );
        result.put( "connection", connectionName );
        return Stream.of(  new MapResult( result ) );
    }

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.reconnect(connectionName) - A method used that reconnects without sending any messages. Broker logging must be enabled or this method will throw an error. For testing purposes." )
    public Stream<MapResult> reconnect( @Name( "connectionName" ) String connectionName )
    {
        BrokerHandler.reconnectAsync( connectionName );
        Map<String,Object> result = new HashMap<>(  );
        result.put( "connection", connectionName );
        return Stream.of(  new MapResult( result ) );
    }


    public enum BrokerType
    {
        RABBITMQ,
        SQS,
        KAFKA
    }

    public static class BrokerHandler
    {
        private static Log neo4jLog;
        private static Boolean loggingEnabled;

        public BrokerHandler( Log log, boolean loggingEnabled )
        {
            neo4jLog = log;
            this.loggingEnabled = loggingEnabled;

            if ( loggingEnabled )
            {
                try
                {
                    startReconnectForDeadOnArrivalConnections();
                    resendMessagesForHealthyConnections();
                }
                catch ( Exception e )
                {

                }
            }
        }

        public static Stream<BrokerMessage> sendMessageToBrokerConnection( String connection, Map<String,Object> message, Map<String,Object> configuration )
                throws Exception
        {
            if ( !doesExist( connection ) )
            {
                throw new IOException( "Broker Exception. Connection '" + connection + "' is not a configured broker connection." );
            }
            BrokerConnection brokerConnection = getConnection( connection );
            try
            {
                if(!brokerConnection.isConnected())
                {
                    throw new Exception(  );
                }

                brokerConnection.checkConnectionHealth();
                Stream<BrokerMessage> brokerMessageStream = brokerConnection.send( message, configuration );

                if ( loggingEnabled )
                {
                    pools.getDefaultExecutorService().execute( (Runnable) () -> retryMessagesForConnectionAsync( connection ) );
                }

                return brokerMessageStream;
            }
            catch ( Exception e )
            {
                if ( loggingEnabled )
                {
                    BrokerLogManager.getBrokerLogger( connection ).error( new BrokerLogger.LogLine.LogEntry( connection, message, configuration ) );
                    brokerConnection.setConnected( false );

                    if ( !brokerConnection.isReconnecting() )
                    {
                        reconnectAndResendAsync( connection );
                    }
                }
            }
            throw new RuntimeException( "Unable to send message to connection '" + connection + "'." );
        }

        public static Stream<BrokerResult> receiveMessageFromBrokerConnection( String connection, Map<String,Object> configuration ) throws IOException
        {
            if ( !doesExist( connection ) )
            {
                throw new IOException( "Broker Exception. Connection '" + connection + "' is not a configured broker connection." );
            }
            return getConnection( connection ).receive( configuration );
        }

        public static Stream<MapResult> flipConnection( String connectionName )
        {
            BrokerConnection brokerConnection = ConnectionManager.getConnection( connectionName );

            brokerConnection.setConnected( !brokerConnection.isConnected() );
            Map<String,Object> result = new HashMap<>(  );
            result.put( "connection", connectionName );
            result.put( "isConnected", brokerConnection.isConnected() );

            return Stream.of(  new MapResult( result ) );
        }

        public static Stream<MapResult> flipReconnect( String connectionName )
        {
            BrokerConnection brokerConnection = ConnectionManager.getConnection( connectionName );

            brokerConnection.setReconnecting(!brokerConnection.isReconnecting() );
            Map<String,Object> result = new HashMap<>(  );
            result.put( "connection", connectionName );
            result.put( "isReconnecting", brokerConnection.isReconnecting() );

            return Stream.of(  new MapResult( result ) );
        }

        public static Stream<MapResult> checkConnection( String connectionName )
        {
            Map<String,Object> result = new HashMap<>(  );
            result.put( "connection", connectionName );
            result.put( "isConnected", ConnectionManager.getConnection( connectionName ).isConnected() );
            return Stream.of(  new MapResult( result ) );
        }

        public static Stream<MapResult> checkReconnect( String connectionName )
        {
            Map<String,Object> result = new HashMap<>(  );
            result.put( "connection", connectionName );
            result.put( "isReconnecting", ConnectionManager.getConnection( connectionName ).isReconnecting() );
            return Stream.of(  new MapResult( result ) );
        }

        private static void resendMessagesForHealthyConnections() throws Exception
        {
            BrokerLogManager.streamBrokerLogInfo().forEach( logInfo -> {
                resendMessagesForConnection( logInfo.getBrokerName() );
            } );
        }

        private static void resendMessagesForConnection(String connectionName )
        {
            if(loggingEnabled)
            {
                try
                {
                    if ( getConnection( connectionName ).isConnected() && BrokerLogManager.getBrokerLogger( connectionName ).calculateNumberOfLogEntries() > 0L )
                    {
                        retryMessagesForConnectionAsync( connectionName );
                    }
                }
                catch ( Exception e )
                {

                }
            }
            else
            {
                throw new RuntimeException( "Broker logging must be enabled to resend messages." );
            }
        }
        private static void retryMessagesForConnectionAsync( String connectionName )
        {
            retryMessagesForConnectionAsync( connectionName, 0L );
        }

        public static void retryMessagesForConnectionAsync( String connectionName, Long numToSend )
        {
            try
            {
                if ( getConnection( connectionName ).isConnected() )
                {
                    pools.getDefaultExecutorService().execute( () -> {
                        try(Stream<BrokerLogManager.LogLine.LogInfo> logInfoStream = BrokerLogManager.readBrokerLogLine( connectionName ))
                        {
                            // Start streaming the lines back from the BrokerLogManager.
                            BrokerLogManager.LogLine.LogInfo logInfo = logInfoStream.findFirst().get();

                            AtomicLong nextLinePointer = new AtomicLong( logInfo.getNextMessageToSend() );
                            AtomicLong numSent = new AtomicLong( 0 );

                            try(Stream<BrokerLogger.LogLine.LogEntry> logEntryStream = BrokerLogger.streamLogLines( logInfo ).map( logLine -> logLine.getLogEntry() ))
                            {

                                for ( BrokerLogger.LogLine.LogEntry logEntry : logEntryStream.collect( Collectors.toList()) )
                                {
                                    neo4jLog.info( "APOC Broker: Resending message for '" + connectionName + "'." );

                                    Boolean resent = resendBrokerMessage( logEntry.getConnectionName(), logEntry.getMessage(), logEntry.getConfiguration() );
                                    if ( resent )
                                    {
                                        //Send successful. Move pointer one line.
                                        nextLinePointer.getAndIncrement();
                                        numSent.getAndIncrement();

                                        // Used for simulating sending exactly numToSend messages.
                                        if ( nextLinePointer.get() - logInfo.getNextMessageToSend() == numToSend )
                                        {
                                            break;
                                        }
                                    }
                                    else
                                    {
                                        // Send unsuccessful. Break to stop sending messages.
                                        break;
                                    }
                                }

                                if ( numSent.get() > 0L )
                                {
                                    if ( nextLinePointer.get() == (BrokerLogManager.getBrokerLogger( connectionName ).calculateNumberOfLogEntries()) )
                                    {
                                        // All the messsages have been sent, reset the broker log.
                                        BrokerLogManager.resetBrokerLogger( connectionName );
                                    }
                                    else
                                    {
                                        // The broker has been disconnected before all the messages could be sent.
                                        ConnectionManager.getConnection( connectionName ).setConnected( false );

                                        // Not all the messages have been sent, so update the line pointer.
                                        BrokerLogManager.updateNextMessageToSend( connectionName, nextLinePointer.get() );

                                        // Start attempting to reconnect
                                        if ( !ConnectionManager.getConnection( connectionName ).isReconnecting() )
                                        {
                                            reconnectAndResendAsync( connectionName );
                                        }
                                    }
                                }
                            }
                        }
                        catch ( Exception e )
                        {

                        }
                    } );
                }
            }
            catch ( Exception e )
            {

            }
        }

        private static Boolean resendBrokerMessage( String connection, Map<String,Object> message, Map<String,Object> configuration )
        {
            if ( !doesExist( connection ) )
            {
                throw new RuntimeException( "Broker Exception. Connection '" + connection + "' is not a configured broker connection." );
            }
            try
            {
                getConnection( connection ).send( message, configuration );
            }
            catch ( Exception e )
            {
                return false;
            }
            return true;
        }

        private static void reconnectAndResendAsync( String connectionName )
        {
            pools.getDefaultExecutorService().execute( () -> {
                BrokerConnection reconnect = ConnectionFactory.reconnect( getConnection( connectionName ) );
                neo4jLog.info( "APOC Broker: Connection '" + connectionName + "' reconnected." );
                ConnectionManager.updateConnection( connectionName, reconnect );
                retryMessagesForConnectionAsync( connectionName );
            } );
        }

        private static void reconnectAsync( String connectionName )
        {
            pools.getDefaultExecutorService().execute( () -> {
                BrokerConnection reconnect = ConnectionFactory.reconnect( getConnection( connectionName ) );
                neo4jLog.info( "APOC Broker: Connection '" + connectionName + "' reconnected." );
                ConnectionManager.updateConnection( connectionName, reconnect );
            } );
        }

        private static void startReconnectForDeadOnArrivalConnections()
        {
            ConnectionManager.getConnectionNames().stream().forEach( connectionName -> {
                BrokerConnection connection = ConnectionManager.getConnection( connectionName );
                if(!connection.isConnected() && !connection.isReconnecting())
                {
                    reconnectAndResendAsync( connectionName );
                }
            } );

        }
    }

    public static class BrokerLifeCycle
    {
        private final Log log;
        private final GraphDatabaseAPI db;

        private static final String LOGS_CONFIG = "logs";

        public BrokerLifeCycle(  GraphDatabaseAPI db, Log log)
        {
            this.log = log;
            this.db = db;
        }

        private static String getBrokerConfiguration( String connectionName, String key )
        {
            Map<String,Object> value = configurationMap( "broker." + connectionName );

            if ( value == null )
            {
                throw new RuntimeException( "No apoc.broker." + connectionName + " specified" );
            }
            return (String) value.get( key );
        }

        private static String getLogsConfiguration( String key )
        {
            return (String) (configurationMap( "brokers." + LOGS_CONFIG  )).get( key );
        }


        public void start()
        {
            Map<String,Object> value = configurationMap( "broker." );

            Set<String> connectionList = new HashSet<>();
            Boolean loggingEnabled = false;

            value.forEach( ( configurationString, object ) -> {
                String connectionName = configurationString.split( "\\." )[0];
                connectionList.add( connectionName );
            } );

            if ( Boolean.valueOf( getLogsConfiguration( "enabled" ) ) )
            {
                loggingEnabled = true;
                BrokerLogManager.initializeBrokerLogManager( db, getLogsConfiguration( "dirPath" ),
                        connectionList.stream().filter( connectionName -> Boolean.valueOf( getBrokerConfiguration( connectionName, "enabled" ) ) ).collect( Collectors.toList() ) );
            }

            for ( String connectionName : connectionList )
            {

                BrokerType brokerType = BrokerType.valueOf( StringUtils.upperCase( getBrokerConfiguration( connectionName, "type" ) ) );
                Boolean enabled = Boolean.valueOf( getBrokerConfiguration( connectionName, "enabled" ) );

                if ( enabled )
                {
                    switch ( brokerType )
                    {
                    case RABBITMQ:
                        ConnectionManager.addRabbitMQConnection( connectionName, log, configurationMap( "broker." + connectionName ) );
                        break;
                    case SQS:
                        ConnectionManager.addSQSConnection( connectionName, log, configurationMap( "broker." + connectionName ) );
                        break;
                    case KAFKA:
                        ConnectionManager.addKafkaConnection( connectionName, log, configurationMap( "broker." + connectionName ) );
                        break;
                    default:
                        break;
                    }
                }
            }

            new BrokerHandler( log, loggingEnabled );
        }

        public void stop()
        {
            ConnectionManager.closeConnections();
        }
    }

    private static Map<String,Object> configurationMap( String prefix )
    {
        Configuration config = ApocConfig.apocConfig().getConfig();

        ImmutableConfiguration immutableConfiguration = config.immutableSubset( prefix );

        Map<String,Object> configurationMap = new HashMap<>();

        for ( Iterator<String> it = immutableConfiguration.getKeys(); it.hasNext(); )
        {
            String key = it.next();
            configurationMap.put( key, immutableConfiguration.get( String.class, key ) );
        }
        return configurationMap;
    }
}
