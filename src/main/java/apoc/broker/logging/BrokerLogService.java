package apoc.broker.logging;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Level;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.AbstractLogService;
import org.neo4j.logging.internal.SimpleLogService;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author alexanderiudice
 */
public class BrokerLogService extends AbstractLogService implements Lifecycle
{

    public static final String DEFAULT_LOG_NAME = "tx.log";

    public static class Builder
    {
        private LogProvider userLogProvider = NullLogProvider.getInstance();
        private Consumer<LogProvider> logProviderConsumer = ( logProvider ) -> {
        };
        private Map<String,Level> logLevels = new HashMap<>();
        private Level defaultLevel = Level.INFO;

        private Builder()
        {
        }

        public Builder withUserLogProvider( LogProvider userLogProvider )
        {
            this.userLogProvider = userLogProvider;
            return this;
        }

        public Builder withRotationListener( Consumer<LogProvider> rotationListener )
        {
            this.logProviderConsumer = rotationListener;
            return this;
        }

        public Builder withLevel( String context, Level level )
        {
            this.logLevels.put( context, level );
            return this;
        }

        public Builder withDefaultLevel( Level defaultLevel )
        {
            this.defaultLevel = defaultLevel;
            return this;
        }

        public BrokerLogService inLogsDirectory( FileSystemAbstraction fileSystem, File logsDir, String logName ) throws IOException
        {
            return new BrokerLogService( userLogProvider, fileSystem, new File( logsDir, logName ), logLevels, defaultLevel, logProviderConsumer );
        }
    }

    private final Closeable closeable;
    private final SimpleLogService logService;

    public static Builder withUserLogProvider( LogProvider userLogProvider )
    {
        return new Builder().withUserLogProvider( userLogProvider );
    }

    public static BrokerLogService inLogsDirectory( FileSystemAbstraction fileSystem, File storeDir, String logName ) throws IOException
    {
        if ( logName == null || logName.isEmpty() )
        {
            return inLogsDirectory( fileSystem, storeDir );
        }
        return new Builder().inLogsDirectory( fileSystem, storeDir, logName );
    }

    public static BrokerLogService inLogsDirectory( FileSystemAbstraction fileSystem, File storeDir ) throws IOException
    {
        return new Builder().inLogsDirectory( fileSystem, storeDir, DEFAULT_LOG_NAME );
    }

    private BrokerLogService( LogProvider userLogProvider, FileSystemAbstraction fileSystem, File internalLog, Map<String,Level> logLevels, Level defaultLevel,
            final Consumer<LogProvider> logProviderConsumer ) throws IOException
    {
        if ( !internalLog.getParentFile().exists() )
        {
            fileSystem.mkdirs( internalLog.getParentFile() );
        }

        final FormattedLogProvider.Builder internalLogBuilder =
                FormattedLogProvider.withUTCTimeZone().withDefaultLogLevel( defaultLevel ).withLogLevels( logLevels );

        FormattedLogProvider internalLogProvider;
        OutputStream outputStream = createOrOpenAsOuputStream( fileSystem, internalLog, true );
        internalLogProvider = internalLogBuilder.toOutputStream( outputStream );
        logProviderConsumer.accept( internalLogProvider );
        this.closeable = outputStream;
        this.logService = new SimpleLogService( userLogProvider, internalLogProvider );
    }

    /**
     * Stripped directly from org.neo4j.io.file Files.java
     * Creates a file, or opens an existing file. If necessary, parent directories will be created.
     *
     * @param fileSystem The filesystem abstraction to use
     * @param file The file to create or open
     * @return An output stream
     * @throws IOException If an error occurs creating directories or opening the file
     */
    private static OutputStream createOrOpenAsOuputStream( FileSystemAbstraction fileSystem, File file, boolean append ) throws IOException
    {
        if ( file.getParentFile() != null )
        {
            fileSystem.mkdirs( file.getParentFile() );
        }
        return fileSystem.openAsOutputStream( file, append );
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
    }

    @Override
    public void stop() throws Throwable
    {
    }

    @Override
    public void shutdown() throws Throwable
    {
        closeable.close();
    }

    @Override
    public LogProvider getUserLogProvider()
    {
        return logService.getUserLogProvider();
    }

    @Override
    public LogProvider getInternalLogProvider()
    {
        return logService.getInternalLogProvider();
    }
}
