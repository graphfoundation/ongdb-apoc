package apoc.broker.logging;

import apoc.util.JsonUtil;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author alexanderiudice
 */
public class BrokerLogger
{

    private static final ObjectMapper OBJECT_MAPPER = JsonUtil.OBJECT_MAPPER;

    @JsonAutoDetect
    public static class LogLine
    {
        private String time;
        private String level;
        private String logName;
        private LogEntry logEntry;

        public LogLine()
        {
        }

        public LogLine( String time, String level, String logName, LogEntry logEntry )
        {
            this.time = time;
            this.level = level;
            this.logName = logName;
            this.logEntry = logEntry;
        }

        public LogLine( String logLine )
        {
            String[] splited = logLine.split( "\\s+", 5 );

            time = splited[0] + " " + splited[1];
            level = splited[2];
            logName = splited[3];
            try
            {
                logEntry = OBJECT_MAPPER.readValue( splited[4], LogEntry.class );
            }
            catch ( Exception e )
            {
                logEntry = new LogEntry();
            }
        }

        public String getTime()
        {
            return time;
        }

        public void setTime( String time )
        {
            this.time = time;
        }

        public String getLevel()
        {
            return level;
        }

        public void setLevel( String level )
        {
            this.level = level;
        }

        public String getLogName()
        {
            return logName;
        }

        public void setLogName( String logName )
        {
            this.logName = logName;
        }

        public LogEntry getLogEntry()
        {
            return logEntry;
        }

        public void setLogEntry( LogEntry logEntry )
        {
            this.logEntry = logEntry;
        }

        public String getLogString()
        {
            String result = "";

            result += time + " " + level + " " + logName + " ";
            try
            {
                result += OBJECT_MAPPER.writeValueAsString( logEntry );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( "Unable to write LogEntry as String" );
            }
            return result;
        }

        @JsonAutoDetect
        public static class LogEntry
        {
            private String connectionName;
            private Map<String,Object> message;
            private Map<String,Object> configuration;

            public LogEntry()
            {
                connectionName = "";
                message = new HashMap<>();
                configuration = new HashMap<>();
            }

            public LogEntry( String connectionName, Map<String,Object> message, Map<String,Object> configuration )
            {
                this.connectionName = connectionName;
                this.message = message;
                this.configuration = configuration;
            }

            public String getConnectionName()
            {
                return connectionName;
            }

            public void setConnectionName( String connectionName )
            {
                this.connectionName = connectionName;
            }

            public Map<String,Object> getMessage()
            {
                return message;
            }

            public void setMessage( Map<String,Object> message )
            {
                this.message = message;
            }

            public Map<String,Object> getConfiguration()
            {
                return configuration;
            }

            public void setConfiguration( Map<String,Object> configuration )
            {
                this.configuration = configuration;
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }

                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }

                LogEntry logEntry = (LogEntry) o;

                return new EqualsBuilder().append( connectionName, logEntry.connectionName ).append( message, logEntry.message ).append( configuration,
                        logEntry.configuration ).isEquals();
            }

            @Override
            public int hashCode()
            {
                return new HashCodeBuilder( 17, 37 ).append( connectionName ).append( message ).append( configuration ).toHashCode();
            }

            @Override
            public String toString()
            {
                return new ToStringBuilder( this ).append( "connectionName", connectionName ).append( "message", message ).append( "configuration",
                        configuration ).toString();
            }
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }

            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            LogLine logLine = (LogLine) o;

            return new EqualsBuilder().append( time, logLine.time ).append( level, logLine.level ).append( logName, logLine.logName ).append( logEntry,
                    logLine.logEntry ).isEquals();
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder( 17, 37 ).append( time ).append( level ).append( logName ).append( logEntry ).toHashCode();
        }
    }

    private String dirPath;
    private String logName;
    private GraphDatabaseAPI graphDatabaseAPI;
    private BrokerLogService brokerLogService;

    private File logFile;

    private AtomicLong numLogEntries = new AtomicLong( 0L );
    private final Long retryThreshold = 20L;

    public BrokerLogger( GraphDatabaseAPI api, String dirPath, String connectionName )
    {

        this.dirPath = dirPath;
        this.graphDatabaseAPI = api;
        this.logName = connectionName + ".log";

        try
        {
            logFile = new File( dirPath + logName );
            logFile.createNewFile();

            brokerLogService =
                    BrokerLogService.inLogsDirectory( api.getDependencyResolver().resolveDependency( FileSystemAbstraction.class ), new File( dirPath ),
                            logName );



            // Get the number of log file entries and set numLogEntries.
            setNumLogEntries( calculateNumberOfLogEntries() );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "APOC Broker Exception. Logger failed to initialize." );
        }
    }

    public Stream<LogLine.LogEntry> streamStartingFrom(Long lineNumber ) throws Exception
    {
        try (Stream<String> lines = Files.lines(Paths.get(logFile.getPath()))) {
            return lines.skip(lineNumber).map( LogLine::new ).map( LogLine::getLogEntry );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Could not start streaming from line number " + lineNumber + "." );
        }
    }

    public Stream<List<LogLine.LogEntry>> batchConnectionMessages( String connectionName, int batchSize ) throws Exception
    {

        Stream<String> stream = Files.lines( Paths.get( logFile.getPath() ) );

        final Stream<LogLine.LogEntry> logEntryStream =
                stream.map( LogLine::new ).filter( logLine -> logLine.logEntry.getConnectionName().equals( connectionName ) ).map(
                        logLine -> logLine.getLogEntry() );

        return Lists.partition( logEntryStream.collect( Collectors.toList() ), batchSize ).stream();
    }

    /**
     * Takes in logInfo as a parameter which it then uses to determine where to start streaming the lines from.
     * @param logInfo
     * @return
     * @throws Exception
     */
    public static Stream<LogLine> streamLogLines( BrokerLogManager.LogLine.LogInfo logInfo ) throws Exception
    {
        return Files.lines( Paths.get( logInfo.getFilePath())).skip( logInfo.getNextMessageToSend()).map( LogLine::new );
    }

    public Long calculateNumberOfLogEntries() throws Exception
    {
        return Files.lines( Paths.get( logFile.getPath() ) ).count();
    }

    public void resetFile()
    {
        synchronized ( logFile )
        {
            try
            {
                // Delete the file.
                Files.delete( Paths.get(  logFile.getPath() ) );


                // Remake the file.
                logFile = new File( dirPath + logName );
                logFile.createNewFile();
                brokerLogService = BrokerLogService.inLogsDirectory( graphDatabaseAPI.getDependencyResolver().resolveDependency( FileSystemAbstraction.class ),
                        new File( dirPath ), logName );

                numLogEntries.getAndSet( 0L );
            }
            catch ( Exception e )
            {
            }
        }
    }

    public Boolean IsAtThreshold()
    {
        return (numLogEntries.get() > retryThreshold);
    }

    public void info( LogLine.LogEntry logEntry ) throws Exception
    {
        info( OBJECT_MAPPER.writeValueAsString( logEntry ) );
    }

    public void warn( LogLine.LogEntry logEntry ) throws Exception
    {
        warn( OBJECT_MAPPER.writeValueAsString( logEntry ) );
    }

    public void debug( LogLine.LogEntry logEntry ) throws Exception
    {
        debug( OBJECT_MAPPER.writeValueAsString( logEntry ) );
    }

    public void error( LogLine.LogEntry logEntry ) throws Exception
    {
        error( OBJECT_MAPPER.writeValueAsString( logEntry ) );
    }

    private void info( String msg )
    {
        brokerLogService.getInternalLogProvider().getLog( logName ).info( msg );
        incrementNumLogEntries();
    }

    private void warn( String msg )
    {
        brokerLogService.getInternalLogProvider().getLog( logName ).warn( msg );
        incrementNumLogEntries();
    }

    private void debug( String msg )
    {
        brokerLogService.getInternalLogProvider().getLog( logName ).debug( msg );
        incrementNumLogEntries();
    }

    private void error( String msg )
    {
        brokerLogService.getInternalLogProvider().getLog( logName ).error( msg );
        incrementNumLogEntries();
    }

    public String getDirPath()
    {
        return dirPath;
    }

    public String getLogName()
    {
        return logName;
    }

    public Long incrementNumLogEntries()
    {
        return numLogEntries.getAndIncrement();
    }

    public Long decrementNumLogEntries()
    {
        return numLogEntries.getAndDecrement();
    }

    public Long setNumLogEntries( Long numLogEntries )
    {
        return this.numLogEntries.getAndSet( numLogEntries );
    }

    public Long getNumLogEntries()
    {
        return numLogEntries.get();
    }
}
