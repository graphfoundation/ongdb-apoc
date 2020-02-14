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

        // exchange -> List of routingKeys
        private Map<String,List<String>> bindingsCache = new HashMap<>();

        public RabbitMqConnection( Log log, String connectionName, Map<String,Object> configuration )
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
                this.log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: " + e.toString() );
                connected.set( false );
            }
        }

        @Override
        public Stream<BrokerMessage> send( @Name( "message" ) Map<String,Object> message, @Name( "configuration" ) Map<String,Object> configuration ) throws Exception
        {
            if ( !configuration.containsKey( "exchangeName" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'exchangeName' in parameters missing" );
            }
            if ( !configuration.containsKey( "routingKey" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'routingKey' in parameters missing" );
            }

            String exchangeName = (String) configuration.get( "exchangeName" );
            String routingKey = (String) configuration.get( "routingKey" );

            checkConnectionHealth();

            // Set up basic properties
            Map<String,Object> properties = (Map<String,Object>) configuration.getOrDefault( "amqpProperties", Collections.<String,Object>emptyMap() );
            AMQP.BasicProperties basicProperties = basicPropertiesMapper( properties );

            // (1) Queue checking and creation
            // NOTE: If a queueName is included then it will always create the binding!
            // For optimization remove queueName if RabbitMQ queues/bindings already all setup.
            Boolean setBindingForQueue = false;
            String queueName = (String) configuration.getOrDefault( "queueName", "" );
            if ( !queueName.isEmpty() )
            {
                try
                {
                    channel.queueDeclarePassive( queueName );
                }
                catch ( IOException e )
                {
                    recoverFromChannelError();

                    log.info( "Queue '" + queueName + "' does not exist for RabbitMQ connection '" + connectionName + "'. Creating it now." );

                    // Queue does not exist so create one and setBindingForQueue = true
                    setBindingForQueue = true;

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

                // If we already know the exchange then just create the binding
                if ( isKnownBinding( exchangeName, routingKey ) )
                {
                    channel.queueBind( queueName, exchangeName, routingKey );
                    setBindingForQueue = false;
                }
                else if ( isKnownExchange( exchangeName ) )
                {
                    bindingsCache.get( exchangeName ).add( routingKey );
                    channel.queueBind( queueName, exchangeName, routingKey );
                    setBindingForQueue = false;
                }

            }

            // (2) Send message if exchange is known.  Create exchange if unknown.
            if ( isKnownBinding( exchangeName, routingKey ) )
            {
                // Send Message
                channel.basicPublish( exchangeName, routingKey, basicProperties, objectMapper.writeValueAsBytes( message ) );
            }
            else if ( isKnownExchange( exchangeName ) )
            {
                bindingsCache.get( exchangeName ).add( routingKey );
                channel.basicPublish( exchangeName, routingKey, basicProperties, objectMapper.writeValueAsBytes( message ) );
            }
            else
            {
                // Check if exchange exists
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

                // Add it to the cache
                bindingsCache.put( exchangeName, Arrays.asList( routingKey ) );

                // Check if queue was set up as well, so we can add the binding now that the exchange/routingKey is known
                if ( setBindingForQueue )
                {
                    channel.queueBind( queueName, exchangeName, routingKey );
                }

                // finally send the message
                channel.basicPublish( exchangeName, routingKey, basicProperties, objectMapper.writeValueAsBytes( message ) );
            }

            return Stream.of( new BrokerMessage( connectionName, message, configuration ) );
        }

        @Override
        public Stream<BrokerResult> receive( @Name( "configuration" ) Map<String,Object> configuration ) throws IOException
        {
            if ( !configuration.containsKey( "queueName" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'queueName' in parameters missing" );
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
                            log.error( "Broker Exception. Connection Name: " + connectionName + ". Message retrieved is null. Possibly no messages in the '" +
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
                    log.error( "Broker Exception. Connection Name: " + connectionName + ". Exception when trying to get a message from the '" +
                            configuration.get( "queueName" ) + "' queue." );
                    throw e;
                }
            }

            return Arrays.stream( messageMap.toArray( new BrokerResult[messageMap.size()] ) );
        }

        @Override
        public void stop()
        {
            try
            {
                if ( channel.isOpen() )
                {
                    channel.close();
                }
                connection.close();
            }
            catch ( Exception e )
            {
                log.error( "Broker Exception. Failed to stop(). Connection Name: " + connectionName + ". Error: " + e.toString() );
            }
        }

        @Override
        public void checkConnectionHealth() throws Exception
        {
            if ( connection == null || !connection.isOpen() )
            {
                if ( connected.get() )
                {
                    log.error( "Broker Exception. Connection Name: " + connectionName + ". Connection lost. Attempting to reestablish the connection." );
                }
                throw new RuntimeException( "RabbitMQ connection for '" + connectionName + "' has closed." );
            }

            if ( channel == null || !channel.isOpen() )
            {
                if ( connected.get() )
                {
                    log.error( "Broker Exception. Connection Name: " + connectionName + ". RabbitMQ channel lost. Attempting to create new channel." );
                }
                throw new RuntimeException( "RabbitMQ channel for '" + connectionName + "' has closed." );
            }
        }

        private boolean isKnownExchange( String exchange )
        {
            return bindingsCache.containsKey( exchange );
        }

        private boolean isKnownBinding( String exchange, String routingKey )
        {
            if ( !bindingsCache.containsKey( exchange ) )
            {
                return false;
            }
            return bindingsCache.get( exchange ).contains( routingKey );
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
                log.error( "Failed to recover from channel error. Error: " + e.getMessage() );
                throw new RuntimeException( "Failed to recover from channel error. Error: " + e.getMessage()  );
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
                    throw new RuntimeException( "Invalid 'timestamp' in the RabbitMQ 'properties' configuration." );
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
}
