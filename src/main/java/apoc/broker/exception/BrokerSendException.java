package apoc.broker.exception;

public class BrokerSendException extends RuntimeException
{
    public BrokerSendException( String message )
    {
        super( "[BrokerSendException] " + message );
    }

    public BrokerSendException( String message, Throwable cause )
    {
        super( "[BrokerSendException] " + message, cause );
    }
}
