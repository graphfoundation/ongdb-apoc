package apoc.broker;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Name;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * @author alexanderiudice
 */
public class KafkaConnectionFactory implements ConnectionFactory
{

    private KafkaConnectionFactory()
    {
    }

    public static KafkaConnection createConnection( String connectionName, Log log, Map<String,Object> configuration )
    {
        return new KafkaConnection( log, connectionName, configuration );
    }

    public static class KafkaConnection implements BrokerConnection
    {
        private static final Integer pollSecondsDefault = 1;
        private Log log;
        private String connectionName;
        private Map<String,Object> configuration;
        private Properties producerProperties;
        private Properties consumerProperties;
        

        private AtomicBoolean connected = new AtomicBoolean( false );
        private AtomicBoolean reconnecting = new AtomicBoolean( false );

        public KafkaConnection( Log log, String connectionName, Map<String,Object> configuration )
        {
            this( log, connectionName, configuration, true );
        }

        public KafkaConnection( Log log, String connectionName, Map<String,Object> configuration, boolean verboseErrorLogging )
        {
            this.log = log;
            this.connectionName = connectionName;
            this.configuration = configuration;

            try
            {
                producerProperties = new Properties(  );
                consumerProperties = new Properties(  );

                producerProperties.setProperty( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, (String) configuration.get( "bootstrap.servers" ) );
                producerProperties.setProperty( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer" );
                producerProperties.setProperty( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer" );

                consumerProperties.setProperty( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, (String) configuration.get( "bootstrap.servers" ) );
                consumerProperties.setProperty( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer" );
                consumerProperties.setProperty( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer" );
                consumerProperties.setProperty( ConsumerConfig.GROUP_ID_CONFIG, (String) configuration.get( "group.id" ) );

                if ( configuration.containsKey( "client.id" ) )
                {
                    consumerProperties.setProperty( ConsumerConfig.CLIENT_ID_CONFIG, (String) configuration.get( "client.id" ) );
                }

                if ( configuration.containsKey( "poll.records.max" ) )
                {
                    consumerProperties.setProperty( ConsumerConfig.MAX_POLL_RECORDS_CONFIG, (String) configuration.get( "poll.records.max" ) );
                }

                connected.set( true );
            }
            catch ( Exception e )
            {
                if ( verboseErrorLogging )
                {
                    BrokerExceptionHandler.brokerConnectionInitializationException( "Failed to initialize Kafka connection '" + connectionName + "'.", e );
                    log.warn( "APOC Broker: Initializing Kafka connection '" + connectionName + "' will be retried." );
                }
                connected.set( false );
            }
        }

        @Override
        public Stream<BrokerMessage> send( @Name( "message" ) Map<String,Object> message, @Name( "configuration" ) Map<String,Object> parameters ) throws Exception
        {
            // Topic and value are required
            if ( !parameters.containsKey( "topic" ) )
            {
                throw BrokerExceptionHandler.brokerSendException( "Broker Exception. Connection Name: " + connectionName + ". Error: 'topic' in parameters missing" );
            }

            String topic = (String) parameters.get( "topic" );

            Integer partition = -1;
            if ( parameters.containsKey( "partition" ) )
            {
                partition = (Integer) parameters.get( "partition" );
            }

            String key = "";
            if ( parameters.containsKey( "key" ) )
            {
                key = (String) parameters.get( "key" );
            }

            ProducerRecord<String,byte[]> producerRecord;
            if ( partition >= 0 && !key.isEmpty() )
            {
                producerRecord = new ProducerRecord<>( topic, partition, key, objectMapper.writeValueAsBytes( message ) );
            }
            else if ( !key.isEmpty() )
            {
                producerRecord = new ProducerRecord<>( topic, key, objectMapper.writeValueAsBytes( message ) );
            }
            else
            {
                producerRecord = new ProducerRecord<>( topic, objectMapper.writeValueAsBytes( message ) );
            }

            try(KafkaProducer<String,byte[]> kafkaProducer = new KafkaProducer<String,byte[]>( producerProperties )){
                kafkaProducer.send( producerRecord );
            }
            catch ( Exception e )
            {
                throw BrokerExceptionHandler.brokerSendException( "Failed to send message to topic '" + topic + "'. Connection Name: " + connectionName + ".",
                        e );
            }


            return Stream.of( new BrokerMessage( connectionName, message, parameters ) );
        }

        @Override
        public Stream<BrokerResult> receive( @Name( "configuration" ) Map<String,Object> configuration ) throws IOException
        {

            List<BrokerResult> responseList = new ArrayList<>();

            // Topic is required
            if ( !configuration.containsKey( "topic" ) )
            {
                throw BrokerExceptionHandler.brokerReceiveException( "Broker Exception. Connection Name: " + connectionName + ". Error: 'topic' in parameters missing" );
            }

            Integer pollSecondsDefault = this.pollSecondsDefault;
            if ( this.configuration.containsKey( "poll.seconds" ) )
            {
                pollSecondsDefault = Integer.parseInt( (String) this.configuration.get( "poll.seconds" ) );
            }
            if ( configuration.containsKey( "pollSeconds" ) )
            {
                pollSecondsDefault = Integer.parseInt( (String) configuration.get( "pollSeconds" ) );
            }

            try ( KafkaConsumer<String,byte[]> kafkaConsumer = new KafkaConsumer<String,byte[]>( consumerProperties ) )
            {

                if ( !kafkaConsumer.subscription().contains( (String) configuration.get( "topic" ) ) )
                {
                    kafkaConsumer.subscribe( Collections.singletonList( (String) configuration.get( "topic" ) ) );
                }

                final ConsumerRecords<String,byte[]> consumerRecords = kafkaConsumer.poll( Duration.ofSeconds( pollSecondsDefault ) );

                consumerRecords.forEach( record -> {
                    try
                    {
                        responseList.add(
                                new BrokerResult( connectionName, Long.toString( record.offset() ), objectMapper.readValue( record.value(), Map.class ) ) );
                    }
                    catch ( Exception e )
                    {
                        BrokerExceptionHandler.brokerReceiveException( "Broker Exception. Connection Name: " + connectionName + ".", e);
                    }
                } );

                kafkaConsumer.commitAsync();
            }
            catch ( Exception e )
            {
                throw BrokerExceptionHandler.brokerReceiveException( "Broker Exception. Connection Name: " + connectionName + ".", e);
            }

            return Arrays.stream( responseList.toArray( new BrokerResult[0] ) );
        }

        @Override
        public void stop()
        {
        }

        @Override
        public void checkConnectionHealth() throws Exception
        {
            try
            {
                KafkaProducer<String,byte[]> kafkaProducer = new KafkaProducer<String,byte[]>( producerProperties );
                kafkaProducer.close();
            }
            catch ( Exception e )
            {
                throw BrokerExceptionHandler.brokerRuntimeException( "Kafka Producer for connection '" + connectionName + "' failed healthcheck.", e );
            }
            try
            {
                KafkaConsumer<String,byte[]> kafkaConsumer = new KafkaConsumer<String,byte[]>( consumerProperties );
                kafkaConsumer.close();
            }
            catch ( Exception e )
            {
                throw BrokerExceptionHandler.brokerRuntimeException( "Kafka Consumer for connection '" + connectionName + "' failed healthcheck.", e );
            }
        }

        @Override
        public String getConnectionName()
        {
            return connectionName;
        }

        @Override
        public Log getLog()
        {
            return log;
        }

        @Override
        public Map<String,Object> getConfiguration()
        {
            return configuration;
        }

        @Override
        public Boolean isConnected()
        {
            return connected.get();
        }

        @Override
        public void setConnected( Boolean connected )
        {
            this.connected.set( connected );
        }

        @Override
        public Boolean isReconnecting()
        {
            return reconnecting.get();
        }

        @Override
        public void setReconnecting( Boolean reconnecting )
        {
            this.reconnecting.set( reconnecting );
        }
    }
}
