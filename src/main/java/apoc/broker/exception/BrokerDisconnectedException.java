package apoc.broker.exception;

import java.io.IOException;

public class BrokerDisconnectedException extends IOException
{
    public BrokerDisconnectedException( String message )
    {
        super( "[BrokerDisconnectedException] " + message );
    }

    public BrokerDisconnectedException( String message, Throwable cause )
    {
        super( "[BrokerDisconnectedException] " + message, cause );
    }
}
