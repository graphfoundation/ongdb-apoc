package apoc;

import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.scheduler.JobScheduler;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class Pools {

    static final String CONFIG_JOBS_SCHEDULED_NUM_THREADS = "jobs.scheduled.num_threads";
    static final String CONFIG_JOBS_POOL_NUM_THREADS = "jobs.pool.num_threads";
    static final String CONFIG_BROKERS_NUM_THREADS = "brokers.num_threads";
    static final String CONFIG_DEBUG_LOG_THREADS = "jobs.debug.logs";

    public final static int DEFAULT_SCHEDULED_THREADS = Runtime.getRuntime().availableProcessors() / 4;
    public final static int DEFAULT_POOL_THREADS = Runtime.getRuntime().availableProcessors() * 2;
    public final static int DEFAULT_BROKERS_THREADS = Runtime.getRuntime().availableProcessors();

    public final static ExecutorService SINGLE = createSinglePool();
    public final static ExecutorService DEFAULT = createDefaultPool();
    public final static ScheduledExecutorService SCHEDULED = createScheduledPool();
    public final static ExecutorService BROKER = createBrokerPool();
    public static JobScheduler ONGDB_SCHEDULER = null;

    static {
        for (ExecutorService service : Arrays.asList(SINGLE, DEFAULT, SCHEDULED, BROKER)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    service.shutdown();
                    service.awaitTermination(10,TimeUnit.SECONDS);
                } catch(Exception ignore) {
                    //
                }
            }));
        }
    }
    private Pools() {
        throw new UnsupportedOperationException();
    }

    public static ExecutorService createDefaultPool() {
        int threads = getNoThreadsInDefaultPool();
        int queueSize = threads * 25;
        return new ThreadPoolExecutorLogger(threads / 2, threads, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(queueSize),
                new CallerBlocksPolicy(), "DEFAULT", threadPoolDebug());
    }

    static class CallerBlocksPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            // Submit again by directly injecting the task into the work queue, waiting if necessary, but also periodically checking if the pool has been
            // shut down.
            FutureTask<Void> task = new FutureTask<>( r, null );
            BlockingQueue<Runnable> queue = executor.getQueue();
            while (!executor.isShutdown()) {
                try {
                    if ( queue.offer( task, 250, TimeUnit.MILLISECONDS ) )
                    {
                        while ( !executor.isShutdown() )
                        {
                            try
                            {
                                task.get( 250, TimeUnit.MILLISECONDS );
                                return; // Success!
                            }
                            catch ( TimeoutException ignore )
                            {
                                // This is fine an expected. We just want to check that the executor hasn't been shut down.
                            }
                        }
                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static int getNoThreadsInDefaultPool() {
        Integer maxThreads = Util.toInteger(ApocConfiguration.get(CONFIG_JOBS_POOL_NUM_THREADS, DEFAULT_POOL_THREADS));
        return Math.max(1, maxThreads == null ? DEFAULT_POOL_THREADS : maxThreads);
    }
    public static int getNoThreadsInScheduledPool() {
        Integer maxThreads = Util.toInteger(ApocConfiguration.get(CONFIG_JOBS_SCHEDULED_NUM_THREADS, DEFAULT_SCHEDULED_THREADS));
        return Math.max(1, maxThreads == null ? DEFAULT_POOL_THREADS : maxThreads);
    }
    public static int getNoThreadsInBrokerPool() {
        Integer maxThreads = Util.toInteger(ApocConfiguration.get(CONFIG_BROKERS_NUM_THREADS, DEFAULT_BROKERS_THREADS));
        return Math.max(1, maxThreads == null ? DEFAULT_BROKERS_THREADS : maxThreads);
    }

    private static ExecutorService createSinglePool() {
        return new ThreadPoolExecutorLogger(1, 1,
                        0L, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>(), "SINGLE", threadPoolDebug() );
    }

    private static ScheduledExecutorService createScheduledPool() {
        return Executors.newScheduledThreadPool(getNoThreadsInScheduledPool());
    }

    private static ExecutorService createBrokerPool() {
        int threads = getNoThreadsInBrokerPool();
        int queueSize = threads * 25;
        return new ThreadPoolExecutorLogger(threads / 2, threads, 30L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(queueSize),
                new CallerBlocksPolicy(), "BROKER", threadPoolDebug() );
    }

    public static <T> Future<Void> processBatch(List<T> batch, GraphDatabaseService db, Consumer<T> action) {
        return DEFAULT.submit((Callable<Void>) () -> {
                try (Transaction tx = db.beginTx()) {
                    batch.forEach(action);
                    tx.success();
                }
                return null;
            }
        );
    }

    public static <T> T force(Future<T> future) throws ExecutionException {
        while (true) {
            try {
                return future.get();
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
        }
    }

    public static Boolean threadPoolDebug()
    {
        return Boolean.valueOf( ApocConfiguration.get( CONFIG_DEBUG_LOG_THREADS, "false" ) );
    }
}
