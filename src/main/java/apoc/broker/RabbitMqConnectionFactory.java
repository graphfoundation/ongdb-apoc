package apoc.broker;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static apoc.broker.RabbitMqConnectionFactory.SendState.BIND;
import static apoc.broker.RabbitMqConnectionFactory.SendState.CACHE_QUEUE_BINDING;
import static apoc.broker.RabbitMqConnectionFactory.SendState.CHECK_KNOWN_BINDING;
import static apoc.broker.RabbitMqConnectionFactory.SendState.CHECK_KNOWN_EXCHANGE;
import static apoc.broker.RabbitMqConnectionFactory.SendState.CHECK_KNOWN_QUEUE;
import static apoc.broker.RabbitMqConnectionFactory.SendState.DECLARE_AND_CACHE_EXCHANGE;
import static apoc.broker.RabbitMqConnectionFactory.SendState.DECLARE_AND_CACHE_QUEUE;
import static apoc.broker.RabbitMqConnectionFactory.SendState.END;
import static apoc.broker.RabbitMqConnectionFactory.SendState.ERROR;
import static apoc.broker.RabbitMqConnectionFactory.SendState.PUBLISH;
import static apoc.broker.RabbitMqConnectionFactory.SendState.START;

/**
 * @author alexanderiudice
 */
public class RabbitMqConnectionFactory implements apoc.broker.ConnectionFactory
{

    private RabbitMqConnectionFactory()
    {
    }

    public static RabbitMqConnection createConnection( String connectionName, Log log, Map<String,Object> configuration )
    {
        return new RabbitMqConnection( log, connectionName, configuration );
    }

    public static class RabbitMqConnection implements BrokerConnection
    {
        private Log log;
        private String connectionName;
        private Map<String,Object> configuration;
        private ConnectionFactory connectionFactory = new ConnectionFactory();
        private Connection connection;
        private Channel channel;

        private AtomicBoolean connected = new AtomicBoolean( false );
        private AtomicBoolean reconnecting = new AtomicBoolean( false );

        /**
         * List of known exchanges. Serves as a cache of already declared and initialized exchanges to reduce overhead.
         */
        private List<String> knownExchanges = new ArrayList<>();

        /**
         * Map of known queues to their bindings. Serves as a cache of already declared and initialized queues and bindings to reduce overhead.
         */
        private Map<String,Map<String,List<String>>> queueBindingsCache = new HashMap<>();

        public RabbitMqConnection( Log log, String connectionName, Map<String,Object> configuration )
        {
            this( log, connectionName, configuration, true );
        }

        public RabbitMqConnection( Log log, String connectionName, Map<String,Object> configuration, boolean verboseErrorLogging )
        {
            this.log = log;
            this.connectionName = connectionName;
            this.configuration = configuration;
            try
            {
                this.connectionFactory.setUsername( (String) configuration.get( "username" ) );
                this.connectionFactory.setPassword( (String) configuration.get( "password" ) );
                this.connectionFactory.setVirtualHost( (String) configuration.get( "vhost" ) );
                this.connectionFactory.setHost( (String) configuration.get( "host" ) );
                this.connectionFactory.setPort( Integer.parseInt( (String) configuration.get( "port" ) ) );

                this.connection = this.connectionFactory.newConnection();

                this.channel = this.connection.createChannel();
                connected.set( true );
            }
            catch ( Exception e )
            {
                if ( verboseErrorLogging )
                {
                    BrokerExceptionHandler.brokerConnectionInitializationException( "Failed to initialize RabbitMQ connection '" + connectionName + "'.", e );
                }
                connected.set( false );
            }
        }

        @Override
        public Stream<BrokerMessage> send( @Name( "message" ) Map<String,Object> message, @Name( "configuration" ) Map<String,Object> configuration ) throws Exception
        {
            if ( !configuration.containsKey( "exchangeName" ) )
            {
                throw BrokerExceptionHandler.brokerSendException( "Broker Exception. Connection Name: " + connectionName + ". Error: 'exchangeName' in parameters missing" );
            }
            if ( !configuration.containsKey( "routingKey" ) )
            {
                throw BrokerExceptionHandler.brokerSendException( "Broker Exception. Connection Name: " + connectionName + ". Error: 'routingKey' in parameters missing" );
            }

            String exchangeName = (String) configuration.get( "exchangeName" );
            String routingKey = (String) configuration.get( "routingKey" );

            checkConnectionHealth();

            // Set up basic properties
            Map<String,Object> properties = (Map<String,Object>) configuration.getOrDefault( "amqpProperties", Collections.<String,Object>emptyMap() );
            AMQP.BasicProperties basicProperties = basicPropertiesMapper( properties );

            // Get queue name
            String queueName = (String) configuration.getOrDefault( "queueName", "" );

            SendState state = START;
            String errorStateMessage = "[RabbitMQ State Machine Error] ";

            // Finite state machine to control program flow. Creates queue/exchange/binding if needed.
            while ( state != END )
            {
                switch ( state )
                {
                case START:
                    if ( queueName.isEmpty() )
                    {
                        state = CHECK_KNOWN_EXCHANGE;
                    }
                    else
                    {
                        state = CHECK_KNOWN_QUEUE;
                    }
                    break;
                case CHECK_KNOWN_EXCHANGE:
                    if ( knownExchanges.contains( exchangeName ) )
                    {
                        if ( queueName.isEmpty() )
                        {
                            state = PUBLISH;
                        }
                        else
                        {
                            state = CACHE_QUEUE_BINDING;
                        }
                    }
                    else
                    {
                        state = DECLARE_AND_CACHE_EXCHANGE;
                    }
                    break;
                case DECLARE_AND_CACHE_EXCHANGE:
                    try
                    {
                        channel.exchangeDeclarePassive( exchangeName );
                    }
                    catch ( IOException e )
                    {
                        recoverFromChannelError();

                        log.info( "Exchange '" + exchangeName + "' does not exist for RabbitMQ connection '" + connectionName + "'. Creating it now." );

                        Map<String,Object> channelConfiguration =
                                (Map<String,Object>) configuration.getOrDefault( "channelConfiguration", Collections.<String,Object>emptyMap() );
                        String channelType = (String) channelConfiguration.getOrDefault( "type", "topic" );
                        Boolean channelDurable = (Boolean) channelConfiguration.getOrDefault( "durable", true );
                        Boolean channelAutoDelete = (Boolean) channelConfiguration.getOrDefault( "autoDelete", false );
                        Map<String,Object> channelArguments =
                                (Map<String,Object>) channelConfiguration.getOrDefault( "arguments", Collections.<String,Object>emptyMap() );

                        // Declare channel
                        channel.exchangeDeclare( exchangeName, channelType, channelDurable, channelAutoDelete, channelArguments );
                    }
                    // cache exchangeName
                    knownExchanges.add( exchangeName );
                    if ( queueName.isEmpty() )
                    {
                        state = PUBLISH;
                    }
                    else
                    {
                        state = CACHE_QUEUE_BINDING;
                    }
                    break;
                case BIND:
                    try
                    {
                        channel.queueBind( queueName, exchangeName, routingKey );
                    }
                    catch ( Exception e )
                    {
                        throw BrokerExceptionHandler.brokerRuntimeException( "Unable to bind exchange and routing-key <" + exchangeName + "," + routingKey + "> to queue '" + queueName + "' ", e );
                    }
                    state = PUBLISH;
                    break;
                case CACHE_QUEUE_BINDING:
                    // Guaranteed that <exchangeName, key> is not known in this state
                    Map<String,List<String>> bindingMap;
                    if ( queueBindingsCache.containsKey( queueName ) )
                    {
                        bindingMap = queueBindingsCache.get( queueName );
                    }
                    else
                    {
                        bindingMap = new HashMap<>();
                    }

                    if ( bindingMap.containsKey( exchangeName ) )
                    {
                        if ( bindingMap.get( exchangeName ).contains( routingKey ) )
                        {
                            errorStateMessage += "Entered state '" + CACHE_QUEUE_BINDING + "' but queue '" + queueName + "' already has binding pair <" + exchangeName + "," + routingKey + ">";
                            state = ERROR;
                            break;
                        }

                        bindingMap.get( exchangeName ).add( routingKey );
                    }
                    else
                    {
                        bindingMap.put( exchangeName, new ArrayList<>( Collections.singletonList( routingKey ) ) );
                    }
                    // cache
                    queueBindingsCache.put( queueName, bindingMap );
                    state = BIND;
                    break;
                case CHECK_KNOWN_QUEUE:
                    if ( queueBindingsCache.containsKey( queueName ) )
                    {
                        state = CHECK_KNOWN_BINDING;
                    }
                    else
                    {
                        state = DECLARE_AND_CACHE_QUEUE;
                    }
                    break;
                case DECLARE_AND_CACHE_QUEUE:
                    try
                    {
                        channel.queueDeclarePassive( queueName );
                    }
                    catch ( IOException e )
                    {
                        recoverFromChannelError();

                        log.info( "Queue '" + queueName + "' does not exist for RabbitMQ connection '" + connectionName + "'. Creating it now." );

                        // Check for config
                        Map<String,Object> queueConfiguration =
                                (Map<String,Object>) configuration.getOrDefault( "queueConfiguration", Collections.<String,Object>emptyMap() );

                        Boolean queueDurable = (Boolean) queueConfiguration.getOrDefault( "durable", true );
                        Boolean queueExclusive = (Boolean) queueConfiguration.getOrDefault( "exclusive", false );
                        Boolean queueAutoDelete = (Boolean) queueConfiguration.getOrDefault( "autoDelete", false );
                        Map<String,Object> queueArguments =
                                (Map<String,Object>) queueConfiguration.getOrDefault( "arguments", Collections.<String,Object>emptyMap() );

                        // Declare
                        channel.queueDeclare( queueName, queueDurable, queueExclusive, queueAutoDelete, queueArguments );
                    }

                    queueBindingsCache.put( queueName, new HashMap<>() );
                    state = CHECK_KNOWN_EXCHANGE;
                    break;
                case CHECK_KNOWN_BINDING:
                    // queue should be known
                    if ( !queueBindingsCache.containsKey( queueName ) )
                    {
                        errorStateMessage += "Entered state '" + CHECK_KNOWN_BINDING.name() + "' but queueBindingsCache does not contain key '" + queueName + "'.";
                        state = ERROR;
                        break;
                    }
                    Map<String,List<String>> bindings = queueBindingsCache.get( queueName );
                    if ( bindings.containsKey( exchangeName ) && bindings.get( exchangeName ).contains( routingKey ) )
                    {
                        state = PUBLISH;
                        break;
                    }
                    // Either the exchange or routingKey was not found
                    state = CHECK_KNOWN_EXCHANGE;
                    break;
                case PUBLISH:
                    try
                    {
                        channel.basicPublish( exchangeName, routingKey, basicProperties, objectMapper.writeValueAsBytes( message ) );
                    }
                    catch ( Exception e )
                    {
                        throw BrokerExceptionHandler.brokerSendException( "Failed to publish message to exchange '" + exchangeName + "'.", e );
                    }
                    state = END;
                    break;
                case ERROR:
                    throw BrokerExceptionHandler.brokerRuntimeException( errorStateMessage );
                case END:
                    // Should not get here
                    break;
                }
            }

            return Stream.of( new BrokerMessage( connectionName, message, configuration ) );
        }

        @Override
        public Stream<BrokerResult> receive( @Name( "configuration" ) Map<String,Object> configuration ) throws IOException
        {
            if ( !configuration.containsKey( "queueName" ) )
            {
                throw BrokerExceptionHandler.brokerReceiveException( "Broker Exception. Connection Name: " + connectionName + ". Error: 'queueName' in parameters missing" );
            }

            Long pollRecordsMax = Long.parseLong( maxPollRecordsDefault );
            if ( this.configuration.containsKey( "poll.records.max" ) )
            {
                pollRecordsMax = Long.parseLong( (String) this.configuration.get( "poll.records.max" ) );
            }
            if ( configuration.containsKey( "pollRecordsMax" ) )
            {
                pollRecordsMax = Long.parseLong( (String) configuration.get( "pollRecordsMax" ) );
            }

            List<GetResponse> messageList = new ArrayList<>();
            List<BrokerResult> messageMap = new ArrayList<>();

            synchronized ( channel )
            {
                try
                {
                    if ( pollRecordsMax == 0L )
                    {
                        pollRecordsMax = channel.messageCount( (String) configuration.get( "queueName" ) );
                    }

                    for ( int i = 0; i < pollRecordsMax; i++ )
                    {
                        GetResponse message = channel.basicGet( (String) configuration.get( "queueName" ), false );
                        if ( message == null )
                        {
                            BrokerExceptionHandler.brokerReceiveException( "Broker Exception. Connection Name: " + connectionName + ". Message retrieved is null. Possibly no messages in the '" +
                                    configuration.get( "queueName" ) + "' queue." );
                            break;
                        }
                        messageList.add( message );
                    }

                    for ( GetResponse getResponse : messageList )
                    {

                        messageMap.add( new BrokerResult( connectionName, Long.toString( getResponse.getEnvelope().getDeliveryTag() ),
                                objectMapper.readValue( getResponse.getBody(), Map.class ) ) );

                        // Ack the message
                        channel.basicAck( getResponse.getEnvelope().getDeliveryTag(), false );
                    }
                }
                catch ( Exception e )
                {
                    throw BrokerExceptionHandler.brokerReceiveException( "Broker Exception. Connection Name: " + connectionName + ". Exception when trying to get a message from the '" +
                            configuration.get( "queueName" ) + "' queue.", e );
                }
            }

            return Arrays.stream( messageMap.toArray( new BrokerResult[0] ) );
        }

        @Override
        public void stop()
        {
            try
            {
                if ( channel != null && channel.isOpen() )
                {
                    channel.close();
                }
                if ( connection != null && connection.isOpen() )
                {
                    connection.close();
                }
            }
            catch ( Exception e )
            {
                BrokerExceptionHandler.brokerRuntimeException( "Broker Exception. Failed to stop(). Connection Name: " + connectionName + ". Error: " + e.toString(), e );
            }
        }

        @Override
        public void checkConnectionHealth() throws Exception
        {
            if ( connection == null || !connection.isOpen() )
            {
                throw BrokerExceptionHandler.brokerRuntimeException( "RabbitMQ connection '" + connectionName + "' failed healthcheck." );
            }

            if ( channel == null || !channel.isOpen() )
            {
                throw BrokerExceptionHandler.brokerRuntimeException( "RabbitMQ channel for '" + connectionName + "' failed healthcheck." );
            }
        }

        private void recoverFromChannelError( )
        {
            try
            {
                if ( channel.isOpen() )
                {
                    channel.close();
                }
                channel = connection.createChannel();
            }
            catch ( Exception e )
            {
                throw BrokerExceptionHandler.brokerConnectionRecoveryException( "Failed to recover from channel error.", e );
            }
        }

        public Log getLog()
        {
            return log;
        }

        public String getConnectionName()
        {
            return connectionName;
        }

        public Map<String,Object> getConfiguration()
        {
            return configuration;
        }

        public ConnectionFactory getConnectionFactory()
        {
            return connectionFactory;
        }

        private AMQP.BasicProperties basicPropertiesMapper( Map<String,Object> propertiesMap )
        {
            AMQP.BasicProperties.Builder amqpProperties = new AMQP.BasicProperties.Builder();

            // deliveryMode defaults to persistence
            amqpProperties.deliveryMode( ((Long) propertiesMap.getOrDefault( "deliveryMode", 2L )).intValue() );

            if ( propertiesMap.containsKey( "contentType" ) )
            {
                amqpProperties.contentType( (String) propertiesMap.get( "contentType" ) );
            }
            if ( propertiesMap.containsKey( "contentEncoding" ) )
            {
                amqpProperties.contentEncoding( (String) propertiesMap.get( "contentEncoding" ) );
            }
            if ( propertiesMap.containsKey( "headers" ) )
            {
                amqpProperties.headers( (Map<String,Object>) propertiesMap.get( "headers" ) );
            }
            if ( propertiesMap.containsKey( "priority" ) )
            {
                amqpProperties.priority( ((Long) propertiesMap.get( "priority" )).intValue() );
            }
            if ( propertiesMap.containsKey( "correlationId" ) )
            {
                amqpProperties.correlationId( (String) propertiesMap.get( "correlationId" ) );
            }
            if ( propertiesMap.containsKey( "replyTo" ) )
            {
                amqpProperties.replyTo( (String) propertiesMap.get( "replyTo" ) );
            }
            if ( propertiesMap.containsKey( "expiration" ) )
            {
                amqpProperties.expiration( (String) propertiesMap.get( "expiration" ) );
            }
            if ( propertiesMap.containsKey( "messageId" ) )
            {
                amqpProperties.messageId( (String) propertiesMap.get( "messageId" ) );
            }
            if ( propertiesMap.containsKey( "timestamp" ) )
            {
                try
                {
                    amqpProperties.timestamp( new SimpleDateFormat().parse( ((String) propertiesMap.get( "timestamp" )) ) );
                }
                catch ( Exception e )
                {
                    throw BrokerExceptionHandler.brokerRuntimeException( "Invalid 'timestamp' in the RabbitMQ 'properties' configuration." );
                }
            }
            if ( propertiesMap.containsKey( "type" ) )
            {
                amqpProperties.type( (String) propertiesMap.get( "type" ) );
            }
            if ( propertiesMap.containsKey( "userId" ) )
            {
                amqpProperties.userId( (String) propertiesMap.get( "userId" ) );
            }
            if ( propertiesMap.containsKey( "appId" ) )
            {
                amqpProperties.appId( (String) propertiesMap.get( "appId" ) );
            }
            if ( propertiesMap.containsKey( "clusterId" ) )
            {
                amqpProperties.clusterId( (String) propertiesMap.get( "clusterId" ) );
            }

            return amqpProperties.build();
        }


        @Override
        public Boolean isConnected()
        {
            return connected.get();
        }

        @Override
        public void setConnected( Boolean connected )
        {
            this.connected.getAndSet( connected );
        }

        @Override
        public Boolean isReconnecting()
        {
            return reconnecting.get();
        }

        @Override
        public void setReconnecting( Boolean reconnecting )
        {
            this.reconnecting.getAndSet( reconnecting );
        }
    }

    // States for the Finite State Machine
    enum SendState
    {
        START,
        CHECK_KNOWN_EXCHANGE,
        DECLARE_AND_CACHE_EXCHANGE,
        BIND,
        CACHE_QUEUE_BINDING,
        CHECK_KNOWN_QUEUE,
        DECLARE_AND_CACHE_QUEUE,
        CHECK_KNOWN_BINDING,
        PUBLISH,
        END,
        ERROR;
    }
}
