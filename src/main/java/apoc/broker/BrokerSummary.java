package apoc.broker;

import java.util.Map;

/**
 * @author alexanderiudice
 */
public class BrokerSummary
{
    public String connectionName;
    public Map<String,Object> configuration;
    public boolean isConnected;
    public boolean isReconnecting;

    public BrokerSummary()
    {
    }

    public BrokerSummary( String connectionName, Map<String,Object> configuration, boolean isConnected, boolean isReconnecting )
    {
        this.connectionName = connectionName;
        this.configuration = configuration;
        this.isConnected = isConnected;
        this.isReconnecting = isReconnecting;
    }

    public String getConnectionName()
    {
        return connectionName;
    }

    public void setConnectionName( String connectionName )
    {
        this.connectionName = connectionName;
    }

    public Map<String,Object> getConfiguration()
    {
        return configuration;
    }

    public void setConfiguration( Map<String,Object> configuration )
    {
        this.configuration = configuration;
    }

    public boolean isConnected()
    {
        return isConnected;
    }

    public void setConnected( boolean connected )
    {
        isConnected = connected;
    }

    public boolean isReconnecting()
    {
        return isReconnecting;
    }

    public void setReconnecting( boolean reconnecting )
    {
        isReconnecting = reconnecting;
    }
}
