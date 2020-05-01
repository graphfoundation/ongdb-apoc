package apoc.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.util.Collections;
import java.util.Map;

public class RabbitMQContainerExtension extends RabbitMQContainer
{
    private static final Logger logger = LoggerFactory.getLogger( RabbitMQContainerExtension.class );

    public static final int DEFAULT_AMQP_PORT = 5672;

    @Override
    public void start()
    {
        super.start();
    }

    public RabbitMQContainerExtension withLogging()
    {
        withLogConsumer( new Slf4jLogConsumer( logger ) );
        return this;
    }

    public RabbitMQContainerExtension durableQueue( String name )
    {
        return durableQueue( name, Collections.emptyMap() );
    }

    public RabbitMQContainerExtension durableQueue( String name, Map<String,Object> args )
    {
        this.withQueue( name, false, true, args );
        return this;
    }

    public RabbitMQContainerExtension durableExchange( String name )
    {
        return durableExchange( name, Collections.emptyMap() );
    }

    public RabbitMQContainerExtension durableExchange( String name, Map<String,Object> args )
    {
        this.withExchange( name, "direct", false, false, true, args );
        return this;
    }

    public RabbitMQContainerExtension queueBinding( String src, String dst, String key )
    {
        this.withBinding( src, dst, Collections.emptyMap(), key, "queue" );
        return this;
    }

    @Override
    public RabbitMQContainerExtension withNetwork( Network network )
    {
        return (RabbitMQContainerExtension) super.withNetwork( network );
    }

    @Override
    public RabbitMQContainerExtension withNetworkAliases( String... aliases )
    {
        return (RabbitMQContainerExtension) super.withNetworkAliases( aliases );
    }
}
