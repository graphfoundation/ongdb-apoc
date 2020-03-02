package apoc.broker.exception;

public class BrokerRuntimeException extends RuntimeException
{
    public BrokerRuntimeException( String message )
    {
        super( "[BrokerRuntimeException] " + message );
    }

    public BrokerRuntimeException( String message, Throwable cause )
    {
        super( "[BrokerRuntimeException] " + message, cause );
    }
}
