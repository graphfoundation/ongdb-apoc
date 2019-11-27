package apoc.broker;

import apoc.result.MapResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.broker.ConnectionManager.doesExist;

/**
 * Integrates the various broker pieces together.
 *
 * @author alexanderiudice
 * @since 2018.09
 */
public class BrokerProcedures
{

    @Context
    public BrokerHandler brokerHandler;

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.send(connectionName, message, configuration) - Send a message to the broker associated with the connectionName namespace. Takes in parameter which are dependent on the broker being used." )
    public Stream<BrokerMessage> send( @Name( "connectionName" ) String connectionName, @Name( "message" ) Map<String,Object> message,
            @Name( "configuration" ) Map<String,Object> configuration ) throws Exception
    {
        checkIfExists( connectionName );
        return brokerHandler.sendMessageToBrokerConnection( connectionName, message, configuration );
    }

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.receive(connectionName, configuration) - Receive a message from the broker associated with the connectionName namespace. Takes in a configuration map which is dependent on the broker being used." )
    public Stream<BrokerResult> receive( @Name( "connectionName" ) String connectionName, @Name( "configuration" ) Map<String,Object> configuration )
            throws IOException
    {
        checkIfExists( connectionName );
        return brokerHandler.receiveMessageFromBrokerConnection( connectionName, configuration );
    }

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.flipConnection(connectionName) - A method used for flipping the connection between on and off. For testing purposes." )
    public Stream<MapResult> flipConnection( @Name( "connectionName" ) String connectionName ) throws IOException
    {
        checkIfExists( connectionName );
        return brokerHandler.flipConnection( connectionName );
    }

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.flipReconnect(connectionName) - A method used for flipping the reconnect between on and off. Turn it to TRUE to stop reconnect from happening. For testing purposes." )
    public Stream<MapResult> flipReconnect( @Name( "connectionName" ) String connectionName ) throws IOException
    {
        checkIfExists( connectionName );
        return brokerHandler.flipReconnect( connectionName );
    }

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.checkConnection(connectionName) - A method used for checking the connection of a specified namespace. For testing purposes." )
    public Stream<MapResult> checkConnection( @Name( "connectionName" ) String connectionName ) throws IOException
    {
        checkIfExists( connectionName );
        return brokerHandler.checkConnection( connectionName );
    }

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.checkReconnect(connectionName) - A method used for checking the reconnection status of a specified namespace. For testing purposes." )
    public Stream<MapResult> checkReconnect( @Name( "connectionName" ) String connectionName ) throws IOException
    {
        checkIfExists( connectionName );
        return brokerHandler.checkReconnect( connectionName );
    }

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.resendMessages(connectionName) - A method used that resends the messages for the specified connection." +
            " Messages are retrieved from the logging file associated with that connection. Broker logging must be enabled or this method will throw an error. For testing purposes." )
    public Stream<MapResult> resendMessages( @Name( "connectionName" ) String connectionName, @Name( value = "numToSend", defaultValue = "0" ) Long numToSend ) throws IOException
    {
        checkIfExists( connectionName );

        brokerHandler.retryMessagesForConnectionAsync( connectionName, numToSend );
        Map<String,Object> result = new HashMap<>();
        result.put( "connection", connectionName );
        return Stream.of( new MapResult( result ) );
    }

    @Procedure( mode = Mode.READ )
    @Description( "apoc.broker.reconnect(connectionName) - A method used that reconnects without sending any messages. Broker logging must be enabled or this method will throw an error. For testing purposes." )
    public Stream<MapResult> reconnect( @Name( "connectionName" ) String connectionName ) throws IOException
    {
        checkIfExists( connectionName );

        brokerHandler.reconnectAsync( connectionName );
        Map<String,Object> result = new HashMap<>();
        result.put( "connection", connectionName );
        return Stream.of( new MapResult( result ) );
    }

    private void checkIfExists( String connectionName ) throws IOException
    {
        if ( !doesExist( connectionName ) )
        {
            throw new IOException( "Broker Exception. Connection '" + connectionName + "' is not a configured broker connection." );
        }
    }
}
