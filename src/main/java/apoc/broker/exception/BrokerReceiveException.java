package apoc.broker.exception;

public class BrokerReceiveException extends RuntimeException
{
    public BrokerReceiveException( String message )
    {
        super( "[BrokerReceiveException] " + message );
    }

    public BrokerReceiveException( String message, Throwable cause )
    {
        super( "[BrokerReceiveException] " + message, cause );
    }
}
