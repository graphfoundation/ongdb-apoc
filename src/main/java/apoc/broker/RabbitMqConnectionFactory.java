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
            }
        }

        public RabbitMqConnection( Log log, String connectionName, Map<String,Object> configuration, ConnectionFactory connectionFactory )
        {
            this.log = log;
            this.connectionName = connectionName;
            this.configuration = configuration;
            this.connectionFactory = connectionFactory;

            try
            {
                this.connection = this.connectionFactory.newConnection();
                this.channel = this.connection.createChannel();
            }
            catch ( Exception e )
            {
                this.log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: " + e.toString() );
            }
        }

        @Override
        public Stream<BrokerMessage> send( @Name( "message" ) Map<String,Object> message, @Name( "configuration" ) Map<String,Object> configuration ) throws Exception
        {
            if ( !configuration.containsKey( "exchangeName" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'exchangeName' in parameters missing" );
            }
            if ( !configuration.containsKey( "queueName" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'queueName' in parameters missing" );
            }
            if ( !configuration.containsKey( "routingKey" ) )
            {
                log.error( "Broker Exception. Connection Name: " + connectionName + ". Error: 'routingKey' in parameters missing" );
            }

            String exchangeName = (String) configuration.get( "exchangeName" );
            String queueName = (String) configuration.get( "queueName" );
            String routingKey = (String) configuration.get( "routingKey" );

            checkConnectionHealth();

            // Set up basic properties
            Map<String,Object> properties = (Map<String,Object>) configuration.getOrDefault( "properties", Collections.<String,Object>emptyMap() );
            AMQP.BasicProperties basicProperties = basicPropertiesMapper( properties );

            // Ensure the exchange and queue are declared.
            channel.exchangeDeclare( exchangeName, "topic", true );
            channel.queueDeclarePassive( queueName );

            // Ensure the exchange and queue are bound by the routing key.
            channel.queueBind( queueName, exchangeName, routingKey );

            // Get the message bytes and send the message bytes.
            channel.basicPublish( exchangeName, routingKey, basicProperties, objectMapper.writeValueAsBytes( message ) );

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
                channel.close();
                connection.close();
            }
            catch ( Exception e )
            {
                log.error( "Broker Exception. Failed to stop(). Connection Name: " + connectionName + ". Error: " + e.toString() );
            }
        }

        public void checkConnectionHealth() throws Exception
        {
            if ( connection == null || !connection.isOpen() )
            {
                if(connected.get())
                {
                    log.error( "Broker Exception. Connection Name: " + connectionName + ". Connection lost. Attempting to reestablish the connection." );
                }
                this.connection = connectionFactory.newConnection();
            }

            if ( channel == null || !channel.isOpen() )
            {
                if(connected.get())
                {
                    log.error( "Broker Exception. Connection Name: " + connectionName + ". RabbitMQ channel lost. Attempting to create new channel." );
                }
                channel = connection.createChannel();
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

        private AMQP.BasicProperties basicPropertiesMapper( Map<String,Object> propertiesMap ) throws Exception
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
                amqpProperties.timestamp( new SimpleDateFormat().parse( ((String) propertiesMap.get( "timestamp" )) ) );
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
