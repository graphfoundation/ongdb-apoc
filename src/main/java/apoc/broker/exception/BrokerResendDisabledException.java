package apoc.broker.exception;

import java.io.IOException;

public class BrokerResendDisabledException extends IOException
{
    public BrokerResendDisabledException( String message )
    {
        super( "[BrokerResendDisabledException] " + message );
    }

    public BrokerResendDisabledException( String message, Throwable cause )
    {
        super( "[BrokerResendDisabledException] " + message, cause );
    }
}
