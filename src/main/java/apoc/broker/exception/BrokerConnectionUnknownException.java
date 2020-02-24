package apoc.broker.exception;

public class BrokerConnectionUnknownException extends RuntimeException
{
    public BrokerConnectionUnknownException( String message )
    {
        super( "[BrokerConnectionUnknownException] " + message );
    }

    public BrokerConnectionUnknownException( String message, Throwable cause )
    {
        super( "[BrokerConnectionUnknownException] " + message, cause );
    }
}
