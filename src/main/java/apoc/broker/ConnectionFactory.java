package apoc.broker;

import java.util.Random;

/**
 * @author alexanderiudice
 */
public interface ConnectionFactory
{
    static BrokerConnection recreateConnection( BrokerConnection brokerConnection ) throws Exception
    {
        BrokerConnection reconnect;

        if ( brokerConnection instanceof RabbitMqConnectionFactory.RabbitMqConnection )
        {
            reconnect = new RabbitMqConnectionFactory.RabbitMqConnection( brokerConnection.getLog(), brokerConnection.getConnectionName(),
                    brokerConnection.getConfiguration() );
        }
        else if ( brokerConnection instanceof SqsConnectionFactory.SqsConnection )
        {
            reconnect = new SqsConnectionFactory.SqsConnection( brokerConnection.getLog(), brokerConnection.getConnectionName(),
                    brokerConnection.getConfiguration() );
        }
        else //if ( brokerConnection instanceof KafkaConnectionFactory.KafkaConnection )
        {
            reconnect = new KafkaConnectionFactory.KafkaConnection( brokerConnection.getLog(), brokerConnection.getConnectionName(),
                    brokerConnection.getConfiguration() );
        }

        reconnect.checkConnectionHealth();
        return reconnect;
    }

    /**
     * Reconnects the brokerConnection by attempting to create a new connection. Uses exponential backoff up to a maximum of 16 seconds.
     * @param brokerConnection
     * @return
     */
    static BrokerConnection createConnectionExponentialBackoff( BrokerConnection brokerConnection )
    {
        brokerConnection.setReconnecting( true );

        int low = 1;
        int high = 1000;
        int n = 0;
        Random r = new Random();
        BrokerConnection newBrokerConnection = null;

        while (brokerConnection.isReconnecting())
        {
            try
            {
                newBrokerConnection = recreateConnection( brokerConnection );
                brokerConnection.setReconnecting( false );
            }
            catch ( Exception e )
            {

                // Keep adding time with a 16 second max between retries
                if ( n < 4 )
                {
                    n++;
                }

                // Wait an indeterminate amount of time (range determined by n)
                try
                {
                    Thread.sleep( ((int) Math.round( Math.pow( 2, n ) ) * 1000) + (r.nextInt( high - low ) + low) );
                }
                catch ( InterruptedException ignored )
                {
                    // Ignoring interruptions in the Thread sleep so that
                    // retries continue
                }
            }
        }
        return newBrokerConnection;
    }
}
