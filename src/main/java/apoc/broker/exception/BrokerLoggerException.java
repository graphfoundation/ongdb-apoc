package apoc.broker.exception;

public class BrokerLoggerException extends RuntimeException
{

    public BrokerLoggerException( String message )
    {
        super( "[BrokerLoggerException] " + message );
    }

    public BrokerLoggerException( String message, Throwable cause )
    {
        super( "[BrokerLoggerException] " + message, cause );
    }
}
