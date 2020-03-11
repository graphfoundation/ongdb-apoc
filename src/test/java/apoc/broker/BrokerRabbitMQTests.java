package apoc.broker;

import apoc.container.RabbitMQContainerExtension;
import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import com.google.common.collect.ImmutableMap;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static apoc.container.RabbitMQContainerExtension.DEFAULT_AMQP_PORT;
import static apoc.util.TestContainerUtil.cleanBuild;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.executeGradleTasks;
import static apoc.util.TestUtil.isTravis;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;

public class BrokerRabbitMQTests
{
    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    private static RabbitMQContainerExtension rabbitMQContainerExtension;
    private static ConnectionFactory connectionFactory;
    private static Connection connection;
    private static Channel channel;

    private static String RABBITMQ = "rabbitmq";

    private static String QUEUE_NAME = "test.queue";
    private static String EXCHANGE_NAME = "test.exchange";
    private static String KEY_NAME = "test.key";

    private static Map<String,Object> TEST_MESSAGE = ImmutableMap.of( "test", 1 );
    private static String uniquePostfix = RandomStringUtils.randomAlphabetic( 4 );

    private static String SEND = "CALL apoc.broker.send( $connectionName, $message, $config )";
    private static String RECEIVE = "CALL apoc.broker.receive( $connectionName, $config )";

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        assumeFalse( isTravis() );

        // Set up test containers network so Neo4j can communicate with RabbitMQ
        Network network = Network.newNetwork();

        // Set up RabbitMQ test container
        TestUtil.ignoreException( () -> {
            rabbitMQContainerExtension = new RabbitMQContainerExtension()
                    .withLogging()
                    .durableExchange( EXCHANGE_NAME )
                    .durableQueue( QUEUE_NAME )
                    .queueBinding( EXCHANGE_NAME, QUEUE_NAME, KEY_NAME )
                    .withNetwork( network )
                    .withNetworkAliases( RABBITMQ );
            rabbitMQContainerExtension.start();
        }, Exception.class);
        assumeNotNull(rabbitMQContainerExtension);



        TestUtil.ignoreException(() -> {
            executeGradleTasks("clean", "shadow");
            neo4jContainer = createEnterpriseDB(true)
                    .withoutAuthentication()
                    .withNeo4jConfig( "apoc.broker.rabbitmq.type", "RABBITMQ" )
                    .withNeo4jConfig( "apoc.broker.rabbitmq.enabled", "true" )
                    .withNeo4jConfig( "apoc.broker.rabbitmq.host", RABBITMQ )
                    .withNeo4jConfig( "apoc.broker.rabbitmq.port", Integer.toString(  5672 ) )
                    .withNeo4jConfig( "apoc.broker.rabbitmq.vhost", "/" )
                    .withNeo4jConfig( "apoc.broker.rabbitmq.username", "guest" )
                    .withNeo4jConfig( "apoc.broker.rabbitmq.password", "guest" )
                    .withNeo4jConfig( "apoc.brokers.logs.enabled", "true" )
                    .withNeo4jConfig( "apoc.brokers.logs.dirPath", "/var/lib/neo4j/logs/" )
                    .withNetwork( network )
                    .withNetworkAliases( "neo4j" );
            neo4jContainer.start();
        }, Exception.class );
        assumeNotNull( neo4jContainer );

        session = neo4jContainer.getSession();

        // Set up ConnectionFactory, Connection, and Channel for Asserting during tests.
        setupRabbitMQObjects();
    }

    private static void setupRabbitMQObjects() throws IOException, TimeoutException
    {
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost( rabbitMQContainerExtension.getContainerIpAddress() );
        connectionFactory.setPort( rabbitMQContainerExtension.getMappedPort( DEFAULT_AMQP_PORT ) );
        connection = connectionFactory.newConnection();
        channel = connection.createChannel();
    }

    @AfterClass
    public static void tearDown()
    {
        if ( neo4jContainer != null )
        {
            neo4jContainer.close();
        }

        if ( rabbitMQContainerExtension != null )
        {
            rabbitMQContainerExtension.stop();
        }

        cleanBuild();
    }

    @Before
    public void setUp() throws Exception
    {
        uniquePostfix = RandomStringUtils.randomAlphabetic( 4 );

        if ( !rabbitMQContainerExtension.isRunning() )
        {
            rabbitMQContainerExtension.start();
            setupRabbitMQObjects();
        }
        else
        {
            // If the connection was severed then create a new connection.
            if ( !connection.isOpen() )
            {
                connection = connectionFactory.newConnection();
            }
            // Always set up a new channel per test
            else
            {
                channel = connection.createChannel();
            }
        }
    }

    @Test
    public void test_broker_rabbit_assertSendCreatesExchange()
    {
        Assert.assertNotNull( channel );

        final String exchangeName = applyPostfix( EXCHANGE_NAME );
        final String queueName = QUEUE_NAME;
        final String routingKey = applyPostfix( KEY_NAME );
        final Map<String,Object> config = ImmutableMap.of( "exchangeName", exchangeName, "queueName", queueName, "routingKey", routingKey );

        // Ensure the exchange does not already exist
        try
        {
            channel.exchangeDeclarePassive( exchangeName );
            Assert.fail("Exchange was already created before executing apoc.broker.send");
        }
        catch ( IOException ignored )
        {
            // Expected IOException. RabbitMQ channel will be closed from the error, need to create a new one.
            try
            {
                channel = connection.createChannel();
            }
            catch ( IOException e )
            {
                Assert.fail("Unexpected IOException while recreating channel.");
            }
        }

        // Run apoc.broker.send to create the exchange
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( SEND, ImmutableMap.of( "connectionName", RABBITMQ, "config", config, "message", TEST_MESSAGE ) );
        }


        // Verify the exchange was created.
        try
        {
            channel.exchangeDeclarePassive( exchangeName );
        }
        catch ( IOException e )
        {
            Assert.fail( "RabbitMQ exchange '" + exchangeName + "' was not created by apoc.broker.send" );
        }
    }


    @Test
    public void test_broker_rabbit_assertSendCreatesQueue()
    {
        Assert.assertNotNull( channel );

        final String exchangeName = EXCHANGE_NAME;
        final String queueName = applyPostfix( QUEUE_NAME );
        final String routingKey = KEY_NAME;
        final Map<String,Object> config = ImmutableMap.of( "exchangeName", exchangeName, "queueName", queueName, "routingKey", routingKey );

        // Ensure the queue does not already exist
        try
        {
            channel.queueDeclarePassive( queueName );
            Assert.fail("Queue was already created before executing apoc.broker.send");
        }
        catch ( IOException ignored )
        {
            // Expected IOException. RabbitMQ channel will be closed from the error, need to create a new one.
            try
            {
                channel = connection.createChannel();
            }
            catch ( IOException e )
            {
                Assert.fail("Unexpected IOException while recreating channel.");
            }
        }

        // Run apoc.broker.send to create the queue
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( SEND, ImmutableMap.of( "connectionName", RABBITMQ, "config", config, "message", TEST_MESSAGE ) );
        }


        // Verify the exchange was created.
        try
        {
            channel.queueDeclarePassive( queueName );
        }
        catch ( IOException e )
        {
            Assert.fail( "RabbitMQ queue '" + queueName + "' was not created by apoc.broker.send" );
        }
    }

    @Test
    public void test_broker_rabbit_assertSend() throws IOException
    {
        Assert.assertNotNull( channel );

        final String exchangeName = EXCHANGE_NAME;
        final String queueName = QUEUE_NAME;
        final String routingKey = KEY_NAME;
        final Map<String,Object> config = ImmutableMap.of( "exchangeName", exchangeName, "queueName", queueName, "routingKey", routingKey );

        // Set up temporary consumer
        final boolean[] messageWasReceived = new boolean[1];
        channel.basicConsume( queueName, new DefaultConsumer( channel )
        {
            @Override
            public void handleDelivery( String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body ) throws IOException
            {
                messageWasReceived[0] = Arrays.equals( body, "{\"test\":1}".getBytes() );
            }
        } );

        // Run apoc.broker.send and publish a test message
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( SEND, ImmutableMap.of( "connectionName", RABBITMQ, "config", config, "message", TEST_MESSAGE ) );
        }

        // check the message was received
        Assert.assertTrue( "The message was received", Unreliables.retryUntilSuccess( 5, TimeUnit.SECONDS, () -> {
            if ( !messageWasReceived[0] )
            {
                throw new IllegalStateException( "Message not received yet" );
            }
            return true;
        } ) );
    }


    @Test
    public void test_broker_rabbit_assertReconnectAndResend() throws IOException, TimeoutException, InterruptedException
    {
        Assert.assertNotNull( channel );
        final String exchangeName = applyPostfix( EXCHANGE_NAME );
        final String queueName = applyPostfix( QUEUE_NAME );
        final String routingKey = applyPostfix( KEY_NAME );
        final Map<String,Object> config = ImmutableMap.of( "exchangeName", exchangeName, "queueName", queueName, "routingKey", routingKey );

        boolean runtimeException = false;

        // Stop RabbitMQContainer to simulate a crash
        rabbitMQContainerExtension.stop();

        // Run apoc.broker.send
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( SEND, ImmutableMap.of( "connectionName", RABBITMQ, "config", config, "message", TEST_MESSAGE ) );
        }
        catch ( ClientException ignored )
        {
            // Expected ClientException
            runtimeException = true;
        }

        if ( !runtimeException )
        {
            Assert.fail( "Expected a thrown ClientException when sending a RMQ message to a closed channel." );
        }

        // Start the rabbitMQContainerExtension up again.
        rabbitMQContainerExtension.start();
        setupRabbitMQObjects();

        // Wait the max amount of time (+ 100ms) for Neo4j to reconnect, then check that messages have been sent.
        waitForBrokerToReconnect();


        // Set up temporary consumer
        final boolean[] messageWasReceived = new boolean[1];
        channel.basicConsume( queueName, new DefaultConsumer( channel )
        {
            @Override
            public void handleDelivery( String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body ) throws IOException
            {
                messageWasReceived[0] = Arrays.equals( body, "{\"test\":1}".getBytes() );
            }
        } );

        // check the message was sent by consuming it
        Assert.assertTrue( "The message was received", Unreliables.retryUntilSuccess( 5, TimeUnit.SECONDS, () -> {
            if ( !messageWasReceived[0] )
            {
                throw new IllegalStateException( "Message not received yet" );
            }
            return true;
        } ) );
    }

    @Test
    public void test_broker_rabbit_assertBrokerLogging() throws IOException, TimeoutException, InterruptedException
    {
        Assert.assertNotNull( channel );
        final String exchangeName = applyPostfix( EXCHANGE_NAME );
        final String queueName = applyPostfix( QUEUE_NAME );
        final String routingKey = applyPostfix( KEY_NAME );
        final Map<String,Object> config = ImmutableMap.of( "exchangeName", exchangeName, "queueName", queueName, "routingKey", routingKey );

        boolean runtimeException = false;

        // Ensure that rabbitmq.log in ONgDB Container is empty
        Container.ExecResult catResult = neo4jContainer.execInContainer( "cat", "/var/lib/neo4j/logs/rabbitmq.log" );
        Assert.assertTrue( catResult.getStdout().isEmpty() );

        // Stop RabbitMQContainer to simulate a crash
        rabbitMQContainerExtension.stop();

        // Run apoc.broker.send
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.run( SEND, ImmutableMap.of( "connectionName", RABBITMQ, "config", config, "message", TEST_MESSAGE ) );
        }
        catch ( ClientException ignored )
        {
            // Expected ClientException
            runtimeException = true;
        }

        if ( !runtimeException )
        {
            Assert.fail( "Expected a thrown ClientException when sending a RMQ message to a closed channel." );
        }

        // Assert that the rabbitmq.log is non-empty
        catResult = neo4jContainer.execInContainer( "cat", "/var/lib/neo4j/logs/rabbitmq.log" );
        Assert.assertFalse( catResult.getStdout().isEmpty() );

        // Start the rabbitMQContainerExtension up again.
        rabbitMQContainerExtension.start();
        setupRabbitMQObjects();
        // Sleep to ensure that Neo4j has reconnected.
        waitForBrokerToReconnect();
    }

    private static String applyPostfix( String s )
    {
        return s + "_" + uniquePostfix;
    }

    private static void waitForBrokerToReconnect() throws InterruptedException
    {
        Thread.sleep( 17 * 1000 + 100 );
    }
}
