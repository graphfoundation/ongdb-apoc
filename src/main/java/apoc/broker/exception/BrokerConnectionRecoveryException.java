package apoc.broker.exception;

public class BrokerConnectionRecoveryException extends RuntimeException
{
    public BrokerConnectionRecoveryException( String message )
    {
        super( "[BrokerConnectionRecoveryException] " + message );
    }

    public BrokerConnectionRecoveryException( String message, Throwable cause )
    {
        super( "[BrokerConnectionRecoveryException] " + message, cause );
    }
}
