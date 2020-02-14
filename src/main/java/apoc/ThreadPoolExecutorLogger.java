package apoc;

import org.neo4j.logging.Log;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolExecutorLogger extends ThreadPoolExecutor
{
    public static Log LOG;
    private Boolean debugLog;
    private String poolName;

    public ThreadPoolExecutorLogger( int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue,
            String poolName, Boolean threadPoolDebug )
    {
        super( corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue );
        this.poolName = poolName;
        this.debugLog = threadPoolDebug;
    }

    public ThreadPoolExecutorLogger( int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue,
            RejectedExecutionHandler handler, String poolName, Boolean threadPoolDebug )
    {
        super( corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler );
        this.poolName = poolName;
        this.debugLog = threadPoolDebug;
    }

    @Override
    protected void beforeExecute( Thread t, Runnable r )
    {
        if ( LOG != null && debugLog )
        {
            LOG.debug( "BeforeExecute Logging:\n" +
                    "Pool: " + this.poolName + "\n" +
                    "Active Thread Count: " + this.getActiveCount() + "\n" +
                    "Thread Name: " + t.getName() + "\n" +
                    "Thread Id: " + t.getId() + "\n" +
                    "Thread Priority: " + t.getPriority() + "\n"
            );
        }
        super.beforeExecute( t, r );
    }

    public Log getLog()
    {
        return LOG;
    }

    public void setLog( Log log )
    {
        this.LOG = log;
    }

    public Map<String,Object> getInfo()
    {
        Map<String,Object> loggingResult = new HashMap<>(  );
        loggingResult.put( "poolName", poolName );
        loggingResult.put( "activeCount", this.getActiveCount() );
        loggingResult.put( "corePoolSize", this.getCorePoolSize() );
        loggingResult.put( "poolSize", this.getPoolSize() );
        loggingResult.put( "largestPoolSize", this.getLargestPoolSize() );
        loggingResult.put( "maximumPoolSize", this.getMaximumPoolSize() );
        loggingResult.put( "taskCount", this.getTaskCount() );
        loggingResult.put( "completedTaskCount", this.getCompletedTaskCount() );

        return loggingResult;
    }
}
