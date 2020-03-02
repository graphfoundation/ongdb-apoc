package apoc.broker;

import apoc.ApocConfiguration;
import apoc.Pools;
import apoc.broker.exception.BrokerConnectionUnknownException;
import apoc.broker.exception.BrokerDisconnectedException;
import apoc.broker.exception.BrokerResendDisabledException;
import apoc.broker.exception.BrokerRuntimeException;
import apoc.broker.exception.BrokerSendException;
import apoc.broker.logging.BrokerLogManager;
import apoc.broker.logging.BrokerLogger;
import apoc.result.MapResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.broker.ConnectionManager.getConnection;

/**
 * Integrates the various broker pieces together.
 *
 * @author alexanderiudice
 * @since 2018.09
 */
public class BrokerIntegration
{

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

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.list() - A method used for listing all connections." )
    public Stream<BrokerSummary> list( )
    {
        return BrokerHandler.listConnections();
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
            BrokerHandler.loggingEnabled = loggingEnabled;

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

        public static Stream<BrokerMessage> sendMessageToBrokerConnection( String connection, Map<String,Object> message, Map<String,Object> configuration )
                throws BrokerConnectionUnknownException
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
                    catch ( BrokerRuntimeException | IOException jpe )
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

        public static Stream<BrokerResult> receiveMessageFromBrokerConnection( String connection, Map<String,Object> configuration ) throws IOException
        {
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

        private static void resendMessagesForConnection( String connectionName ) throws BrokerResendDisabledException
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
                    Pools.BROKER.execute( () -> {
                        try ( Stream<BrokerLogManager.LogLine.LogInfo> logInfoStream = BrokerLogManager.readBrokerLogLine( connectionName ) )
                        {
                            // Start streaming the lines back from the BrokerLogManager.
                            BrokerLogManager.LogLine.LogInfo logInfo = logInfoStream.findFirst().get();

                            AtomicLong nextLinePointer = new AtomicLong( logInfo.getNextMessageToSend() );
                            AtomicLong numSent = new AtomicLong( 0 );
                            AtomicBoolean failedToSend = new AtomicBoolean( false );

                            try ( Stream<BrokerLogger.LogLine.LogEntry> logEntryStream = BrokerLogger.streamLogLines( logInfo ).map(
                                    logLine -> logLine.getLogEntry() ) )
                            {

                                for ( BrokerLogger.LogLine.LogEntry logEntry : logEntryStream.collect( Collectors.toList() ) )
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

        private static void reconnectAndResendAsync( String connectionName )
        {
            BrokerConnection connection = getConnection( connectionName );
            if (!connection.isReconnecting())
            {
                Pools.BROKER.execute( () -> {
                    BrokerConnection newConnection = ConnectionFactory.createConnectionExponentialBackoff( connection );
                    neo4jLog.info( "APOC Broker: Connection '" + connectionName + "' reconnected." );
                    ConnectionManager.updateConnection( connectionName, newConnection );
                    retryMessagesForConnectionAsync( connectionName );
                } );
            }
        }

        private static void reconnectAsync( String connectionName )
        {
            BrokerConnection connection = getConnection( connectionName );
            if (!connection.isReconnecting())
            {
                Pools.BROKER.execute( () -> {
                    BrokerConnection newConnection = ConnectionFactory.createConnectionExponentialBackoff( connection );
                    neo4jLog.info( "APOC Broker: Connection '" + connectionName + "' reconnected." );
                    ConnectionManager.updateConnection( connectionName, newConnection );
                } );
            }
        }

        private static void startReconnectForDeadOnArrivalConnections()
        {
            ConnectionManager.getConnectionNames().stream().forEach( connectionName -> {
                BrokerConnection connection = ConnectionManager.getConnection( connectionName );
                if ( !connection.isConnected() )
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
            BrokerExceptionHandler.log = log;
        }

        private static String getBrokerConfiguration( String connectionName, String key )
        {
            Map<String,Object> value = ApocConfiguration.get( "broker." + connectionName );

            if ( value == null )
            {
                throw new RuntimeException( "No apoc.broker." + connectionName + " specified" );
            }
            return (String) value.get( key );
        }

        private static String getLogsConfiguration( String key )
        {
            return (String) (ApocConfiguration.get( "brokers." + LOGS_CONFIG  )).get( key );
        }


        public void start()
        {
            Map<String,Object> value = ApocConfiguration.get( "broker." );

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
                        ConnectionManager.addRabbitMQConnection( connectionName, log, ApocConfiguration.get( "broker." + connectionName ) );
                        break;
                    case SQS:
                        ConnectionManager.addSQSConnection( connectionName, log, ApocConfiguration.get( "broker." + connectionName ) );
                        break;
                    case KAFKA:
                        ConnectionManager.addKafkaConnection( connectionName, log, ApocConfiguration.get( "broker." + connectionName ) );
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
}
