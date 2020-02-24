package apoc.broker;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.broker.exception.BrokerDisconnectedException;
import apoc.broker.exception.BrokerResendDisabledException;
import apoc.broker.exception.BrokerRuntimeException;
import apoc.broker.exception.BrokerSendException;
import apoc.broker.logging.BrokerLogManager;
import apoc.broker.logging.BrokerLogger;
import apoc.result.MapResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.broker.BrokerHandler.BrokerType.NONE;
import static apoc.broker.ConnectionManager.doesExist;
import static apoc.broker.ConnectionManager.getConnection;

public class BrokerHandler extends LifecycleAdapter
{

    private static final String LOGS_CONFIG = "logs";
    private static final String APOC_BROKERS_LOGS_ENABLED = "apoc.brokers.logs.enabled";
    private static final String APOC_BROKERS_LOGS_DIRPATH = "apoc.brokers.logs.dirPath";
    private static final String LOGS_DIRPATH_DEFAULT = "logs/";

    private static final String APOC_BROKER_PREFIX = "apoc.broker.";
    private static final String ENABLED_SUFFIX = ".enabled";
    private static final String TYPE_SUFFIX = ".type";

    private final GraphDatabaseAPI db;
    private final Log neo4jLog;
    private final Configuration entireConfiguration;
    private final Pools pools;


    private Map<String,Object> brokerConfigMap = new HashMap<>(  );
    private Boolean loggingEnabled;

    public BrokerHandler( GraphDatabaseAPI db, Log log, ApocConfig apocConfig, Pools pools )
    {
        this.db = db;
        this.neo4jLog = log;
        this.entireConfiguration = apocConfig.getConfig();
        this.pools = pools;
        BrokerExceptionHandler.log = log;
    }

    @Override
    public void start()
    {
        brokerConfigMap = configurationMap( APOC_BROKER_PREFIX );
        loggingEnabled = false;

        Set<String> connectionList = new HashSet<>();

        if (entireConfiguration.containsKey(APOC_BROKERS_LOGS_ENABLED))
        {
            loggingEnabled = entireConfiguration.getBoolean( APOC_BROKERS_LOGS_ENABLED );
        }

        brokerConfigMap.forEach( ( configurationString, object ) -> {
            String connectionName = configurationString.split( "\\." )[0];
            connectionList.add( connectionName );
        } );

        if ( loggingEnabled )
        {
            BrokerLogManager.initializeBrokerLogManager( db, entireConfiguration.getString( APOC_BROKERS_LOGS_DIRPATH, LOGS_DIRPATH_DEFAULT ),
                    connectionList.stream().filter( connectionName -> Boolean.valueOf( (String) brokerConfigMap.getOrDefault( connectionName + ENABLED_SUFFIX, "false" ) ) ).collect(
                            Collectors.toList() ) );
        }

        for ( String connectionName : connectionList )
        {

            Boolean enabled = Boolean.valueOf( (String) brokerConfigMap.getOrDefault( connectionName + ENABLED_SUFFIX, "false" ) );

            if ( enabled )
            {
                BrokerType brokerType =
                        BrokerType.valueOf( (String) brokerConfigMap.getOrDefault( connectionName + TYPE_SUFFIX, NONE ));
                switch ( brokerType )
                {
                case RABBITMQ:
                    ConnectionManager.addRabbitMQConnection( connectionName, neo4jLog, configurationMap( APOC_BROKER_PREFIX + connectionName ) );
                    break;
                case SQS:
                    ConnectionManager.addSQSConnection( connectionName, neo4jLog, configurationMap( APOC_BROKER_PREFIX + connectionName ) );
                    break;
                case KAFKA:
                    ConnectionManager.addKafkaConnection( connectionName, neo4jLog, configurationMap( APOC_BROKER_PREFIX + connectionName ) );
                    break;
                case NONE:
                default:
                    break;
                }
            }
        }

        if ( loggingEnabled )
        {
            try
            {
                startReconnectForDeadOnArrivalConnections();
            }
            catch ( Exception e )
            {
                BrokerExceptionHandler.brokerRuntimeException( "Unable to reconnect to dead-on-arrival connections. Error: " + e.getMessage(), e );
            }

            try
            {
                resendMessagesForHealthyConnections();
            }
            catch ( Exception e )
            {
                BrokerExceptionHandler.brokerRuntimeException( "Unable to resend messages to healthy connections.", e );
            }
        }
    }

    @Override
    public void stop()
    {
        ConnectionManager.closeConnections();
    }

    private Map<String,Object> configurationMap( String prefix )
    {
        ImmutableConfiguration immutableConfiguration = entireConfiguration.immutableSubset( prefix );

        Map<String,Object> configurationMap = new HashMap<>();

        for ( Iterator<String> it = immutableConfiguration.getKeys(); it.hasNext(); )
        {
            String key = it.next();
            configurationMap.put( key, immutableConfiguration.get( String.class, key ) );
        }
        return configurationMap;
    }

    public enum BrokerType
    {
        RABBITMQ,
        SQS,
        KAFKA,
        NONE
    }

    public Stream<BrokerMessage> sendMessageToBrokerConnection( String connection, Map<String,Object> message, Map<String,Object> configuration )
            throws Exception
    {
        BrokerConnection brokerConnection = getConnection( connection );
        try
        {
            if ( !brokerConnection.isConnected() )
            {
                throw BrokerExceptionHandler.brokerDisconnectedException( "Broker Connection '" + connection + "' is not connected to its broker." );
            }

            brokerConnection.checkConnectionHealth();
            Stream<BrokerMessage> brokerMessageStream = brokerConnection.send( message, configuration );

            if ( loggingEnabled )
            {
                retryMessagesForConnectionAsync( connection );
            }

            return brokerMessageStream;
        }
        catch ( Exception e )
        {
            BrokerSendException brokerSendException;
            if ( e instanceof BrokerDisconnectedException )
            {
                // No need to log out stacktrace
                brokerSendException = BrokerExceptionHandler.brokerSendException( "Unable to send message to connection '" + connection + "'. Error: " + e.getMessage() );
            }
            else
            {
                brokerSendException =
                        BrokerExceptionHandler.brokerSendException( "Unable to send message to connection '" + connection + "'. Error: " + e.getMessage(), e );
            }

            if ( loggingEnabled )
            {
                try
                {
                    BrokerLogManager.getBrokerLogger( connection ).error( new BrokerLogger.LogLine.LogEntry( connection, message, configuration ) );
                }
                catch ( BrokerRuntimeException | JsonProcessingException jpe )
                {
                    throw BrokerExceptionHandler.brokerRuntimeException( "BrokerLogger was unable to persist unsent message to retry logs.", jpe );
                }
                finally
                {
                    brokerConnection.setConnected( false );
                    reconnectAndResendAsync( connection );
                }
            }
            throw brokerSendException;
        }
    }

    public Stream<BrokerResult> receiveMessageFromBrokerConnection( String connection, Map<String,Object> configuration ) throws IOException
    {
        return getConnection( connection ).receive( configuration );
    }

    public Stream<MapResult> flipConnection( String connectionName )
    {
        BrokerConnection brokerConnection = ConnectionManager.getConnection( connectionName );

        brokerConnection.setConnected( !brokerConnection.isConnected() );
        Map<String,Object> result = new HashMap<>(  );
        result.put( "connection", connectionName );
        result.put( "isConnected", brokerConnection.isConnected() );

        return Stream.of(  new MapResult( result ) );
    }

    public Stream<MapResult> flipReconnect( String connectionName )
    {
        BrokerConnection brokerConnection = ConnectionManager.getConnection( connectionName );

        brokerConnection.setReconnecting(!brokerConnection.isReconnecting() );
        Map<String,Object> result = new HashMap<>(  );
        result.put( "connection", connectionName );
        result.put( "isReconnecting", brokerConnection.isReconnecting() );

        return Stream.of(  new MapResult( result ) );
    }

    public Stream<MapResult> checkConnection( String connectionName )
    {
        Map<String,Object> result = new HashMap<>(  );
        result.put( "connection", connectionName );
        result.put( "isConnected", ConnectionManager.doesExist( connectionName ) ? ConnectionManager.getConnection( connectionName ).isConnected() : false );
        return Stream.of(  new MapResult( result ) );
    }

    public Stream<MapResult> checkReconnect( String connectionName )
    {
        Map<String,Object> result = new HashMap<>(  );
        result.put( "connection", connectionName );
        result.put( "isReconnecting", ConnectionManager.doesExist( connectionName ) ? ConnectionManager.getConnection( connectionName ).isReconnecting() : false );
        return Stream.of(  new MapResult( result ) );
    }

    private void resendMessagesForHealthyConnections() throws Exception
    {
        List<Exception> thrownExceptions = new ArrayList<>();
        BrokerLogManager.streamBrokerLogInfo().forEach( logInfo -> {
            try
            {
                resendMessagesForConnection( logInfo.getBrokerName() );
            }
            catch ( BrokerResendDisabledException | RuntimeException e )
            {
                thrownExceptions.add( e );
            }
        } );

        if ( !thrownExceptions.isEmpty() )
        {
            throw new BrokerRuntimeException( "Errors resending messages on initialization. Exceptions thrown: " +
                    thrownExceptions.stream().map( Throwable::getMessage ).collect( Collectors.joining( ",", "[", "]" ) ) );
        }
    }

    private void resendMessagesForConnection( String connectionName ) throws BrokerResendDisabledException
    {
        if ( loggingEnabled )
        {
            if ( getConnection( connectionName ).isConnected() && BrokerLogManager.getBrokerLogger( connectionName ).calculateNumberOfLogEntries() > 0L )
            {
                retryMessagesForConnectionAsync( connectionName );
            }
        }
        else
        {
            throw BrokerExceptionHandler.brokerResendDisabledException( "Broker logging must be enabled to resend messages." );
        }
    }
    private void retryMessagesForConnectionAsync( String connectionName )
    {
        retryMessagesForConnectionAsync( connectionName, 0L );
    }

    public void retryMessagesForConnectionAsync( String connectionName, Long numToSend )
    {
        try
        {
            if ( getConnection( connectionName ).isConnected() )
            {
                pools.getBrokerExecutorService().execute( () -> {
                    try(Stream<BrokerLogManager.LogLine.LogInfo> logInfoStream = BrokerLogManager.readBrokerLogLine( connectionName ))
                    {
                        // Start streaming the lines back from the BrokerLogManager.
                        BrokerLogManager.LogLine.LogInfo logInfo = logInfoStream.findFirst().get();

                        AtomicLong nextLinePointer = new AtomicLong( logInfo.getNextMessageToSend() );
                        AtomicLong numSent = new AtomicLong( 0 );
                        AtomicBoolean failedToSend = new AtomicBoolean( false );


                        try(Stream<BrokerLogger.LogLine.LogEntry> logEntryStream = BrokerLogger.streamLogLines( logInfo ).map( logLine -> logLine.getLogEntry() ))
                        {

                            for ( BrokerLogger.LogLine.LogEntry logEntry : logEntryStream.collect( Collectors.toList()) )
                            {
                                neo4jLog.debug( "APOC Broker: Resending message for '" + connectionName + "'." );


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
                                    failedToSend.set( true );
                                    break;
                                }
                            }

                            if ( numSent.get() > 0L || failedToSend.get() )
                            {
                                neo4jLog.info( "APOC Broker: Resent " + numSent + " messages for '" + connectionName + "'." );

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
                                    reconnectAndResendAsync( connectionName );
                                }
                            }
                        }
                    }
                    catch ( IOException e )
                    {
                        BrokerExceptionHandler.brokerRuntimeException( "Error in async execute 'retryMessagesForConnectionAsync'. Error: " + e.getMessage(), e );
                    }
                } );
            }
        }
        catch ( Exception e )
        {
            BrokerExceptionHandler.brokerRuntimeException( "Error in method 'retryMessagesForConnectionAsync'. Error: " + e.getMessage(), e );
        }
    }

    private static Boolean resendBrokerMessage( String connection, Map<String,Object> message, Map<String,Object> configuration )
    {
        try
        {
            getConnection( connection ).send( message, configuration );
        }
        catch ( Exception e )
        {
            BrokerExceptionHandler.brokerSendException( "Broker Exception in 'resendBrokerMessage'. Unable to resend message to connection '" + connection + "'. Error: " + e.getMessage(), e );
            return false;
        }
        return true;
    }

    private void reconnectAndResendAsync( String connectionName )
    {
        BrokerConnection connection = getConnection( connectionName );
        if ( !connection.isReconnecting() )
        {
            pools.getBrokerExecutorService().execute( () -> {
                BrokerConnection reconnect = ConnectionFactory.createConnectionExponentialBackoff( connection );
                neo4jLog.info( "APOC Broker: Connection '" + connectionName + "' reconnected." );
                ConnectionManager.updateConnection( connectionName, reconnect );
                retryMessagesForConnectionAsync( connectionName );
            } );
        }
    }

    public void reconnectAsync( String connectionName )
    {
        BrokerConnection connection = getConnection( connectionName );
        if ( !connection.isReconnecting() )
        {
            pools.getBrokerExecutorService().execute( () -> {
                BrokerConnection reconnect = ConnectionFactory.createConnectionExponentialBackoff( getConnection( connectionName ) );
                neo4jLog.info( "APOC Broker: Connection '" + connectionName + "' reconnected." );
                ConnectionManager.updateConnection( connectionName, reconnect );
            } );
        }
    }

    private void startReconnectForDeadOnArrivalConnections()
    {
        ConnectionManager.getConnectionNames().stream().forEach( connectionName -> {
            BrokerConnection connection = ConnectionManager.getConnection( connectionName );
            if ( !connection.isConnected() )
            {
                reconnectAndResendAsync( connectionName );
            }
        } );
    }


    public static Stream<BrokerSummary> listConnections()
    {
        return ConnectionManager.getConnectionNames().stream()
                .map( ConnectionManager::getConnection )
                .map( connection ->
                        new BrokerSummary( connection.getConnectionName(), connection.getConfiguration(), connection.isConnected(),
                                connection.isReconnecting()
                        )
                );
    }
}
