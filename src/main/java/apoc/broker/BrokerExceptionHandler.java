package apoc.broker;

import apoc.broker.exception.BrokerConnectionInitializationException;
import apoc.broker.exception.BrokerConnectionRecoveryException;
import apoc.broker.exception.BrokerConnectionUnknownException;
import apoc.broker.exception.BrokerDisconnectedException;
import apoc.broker.exception.BrokerLoggerException;
import apoc.broker.exception.BrokerReceiveException;
import apoc.broker.exception.BrokerResendDisabledException;
import apoc.broker.exception.BrokerRuntimeException;
import apoc.broker.exception.BrokerSendException;

import java.net.ConnectException;

import org.neo4j.logging.Log;

/**
 * @author alexanderiudice
 * @since 2020.02.20
 * <p>
 * This class is used to log out errors and then instantiate a corresponding Broker exception
 */
public class BrokerExceptionHandler
{
    static Log log;

    private BrokerExceptionHandler()
    {
    }

    public static BrokerDisconnectedException brokerDisconnectedException( String msg )
    {
        return brokerDisconnectedException( msg, null );
    }

    public static BrokerConnectionUnknownException brokerConnectionUnknownException( String msg )
    {
        return brokerConnectionUnknownException( msg, null );
    }

    public static BrokerRuntimeException brokerRuntimeException( String msg )
    {
        return brokerRuntimeException( msg, null );
    }

    public static BrokerSendException brokerSendException( String msg )
    {
        return brokerSendException( msg, null );
    }

    public static BrokerLoggerException brokerLoggerException( String msg )
    {
        return brokerLoggerException( msg, null );
    }

    public static BrokerResendDisabledException brokerResendDisabledException( String msg )
    {
        return brokerResendDisabledException( msg, null );
    }

    public static BrokerConnectionRecoveryException brokerConnectionRecoveryException( String msg )
    {
        return brokerConnectionRecoveryException( msg, null );
    }

    public static BrokerConnectionInitializationException brokerConnectionInitializationException( String msg )
    {
        return brokerConnectionInitializationException( msg, null );
    }

    public static BrokerReceiveException brokerReceiveException( String msg )
    {
        return brokerReceiveException( msg, null );
    }

    public static BrokerDisconnectedException brokerDisconnectedException( String msg, Throwable e )
    {
        BrokerDisconnectedException brokerException;
        if ( e != null )
        {
            brokerException = new BrokerDisconnectedException( msg, e );
            log.error( brokerException.getMessage(), e );
        }
        else
        {
            brokerException = new BrokerDisconnectedException( msg );
            log.error( brokerException.getMessage() );
        }
        return brokerException;
    }

    public static BrokerConnectionUnknownException brokerConnectionUnknownException( String msg, Throwable e )
    {
        BrokerConnectionUnknownException brokerException;
        if ( e != null )
        {
            brokerException = new BrokerConnectionUnknownException( msg, e );
            log.error( brokerException.getMessage(), e );
        }
        else
        {
            brokerException = new BrokerConnectionUnknownException( msg );
            log.error( brokerException.getMessage() );
        }
        return brokerException;
    }

    public static BrokerRuntimeException brokerRuntimeException( String msg, Throwable e )
    {
        BrokerRuntimeException brokerException;
        if ( e != null )
        {
            brokerException = new BrokerRuntimeException( msg, e );
            log.error( brokerException.getMessage(), e );
        }
        else
        {
            brokerException = new BrokerRuntimeException( msg );
            log.error( brokerException.getMessage() );
        }
        return brokerException;
    }

    public static BrokerSendException brokerSendException( String msg, Throwable e )
    {
        BrokerSendException brokerException;
        if ( e != null )
        {
            brokerException = new BrokerSendException( msg, e );
            log.error( brokerException.getMessage(), e );
        }
        else
        {
            brokerException = new BrokerSendException( msg );
            log.error( brokerException.getMessage() );
        }

        return brokerException;
    }

    public static BrokerLoggerException brokerLoggerException( String msg, Throwable e )
    {
        BrokerLoggerException brokerException;
        if ( e != null )
        {
            brokerException = new BrokerLoggerException( msg, e );
            log.error( brokerException.getMessage(), e );
        }
        else
        {
            brokerException = new BrokerLoggerException( msg );
            log.error( brokerException.getMessage() );
        }

        return brokerException;
    }

    public static BrokerResendDisabledException brokerResendDisabledException( String msg, Throwable e )
    {
        BrokerResendDisabledException brokerException;
        if ( e != null )
        {
            brokerException = new BrokerResendDisabledException( msg, e );
            log.error( brokerException.getMessage(), e );
        }
        else
        {
            brokerException = new BrokerResendDisabledException( msg );
            log.error( brokerException.getMessage() );
        }

        return brokerException;
    }

    public static BrokerConnectionRecoveryException brokerConnectionRecoveryException( String msg, Throwable e )
    {
        BrokerConnectionRecoveryException brokerException;
        if ( e != null )
        {
            brokerException = new BrokerConnectionRecoveryException( msg, e );
            log.error( brokerException.getMessage(), e );
        }
        else
        {
            brokerException = new BrokerConnectionRecoveryException( msg );
            log.error( brokerException.getMessage() );
        }

        return brokerException;
    }

    public static BrokerConnectionInitializationException brokerConnectionInitializationException( String msg, Throwable e )
    {
        BrokerConnectionInitializationException brokerException;
        if ( e == null )
        {
            brokerException = new BrokerConnectionInitializationException( msg );
            log.error( brokerException.getMessage() );
        }
        else if ( e instanceof ConnectException )
        {
            brokerException = new BrokerConnectionInitializationException( msg + " " + e );
            log.warn( brokerException.getMessage() );
        }
        else
        {
            brokerException = new BrokerConnectionInitializationException( msg, e );
            log.error( brokerException.getMessage() );
        }

        return brokerException;
    }

    public static BrokerReceiveException brokerReceiveException( String msg, Throwable e )
    {
        BrokerReceiveException brokerException;
        if ( e != null )
        {
            brokerException = new BrokerReceiveException( msg, e );
            log.error( brokerException.getMessage(), e );
        }
        else
        {
            brokerException = new BrokerReceiveException( msg );
            log.error( brokerException.getMessage() );
        }

        return brokerException;
    }
}
