package apoc.broker.exception;

public class BrokerConnectionInitializationException extends RuntimeException
{
    public BrokerConnectionInitializationException( String message )
    {
        super( "[BrokerConnectionInitializationException] " + message );
    }

    public BrokerConnectionInitializationException( String message, Throwable cause )
    {
        super( "[BrokerConnectionInitializationException] " + message, cause );
    }
}
