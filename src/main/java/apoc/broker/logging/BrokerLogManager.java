package apoc.broker.logging;

import apoc.util.JsonUtil;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * @author alexanderiudice
 */
public class BrokerLogManager
{

    private static final ObjectMapper OBJECT_MAPPER = JsonUtil.OBJECT_MAPPER;

    private static String dirPath;
    private static GraphDatabaseAPI graphDatabaseAPI;

    private static ConcurrentHashMap<String,BrokerLogger> nameToLogMap;
    private static File brokerLog;
    private static BrokerLogService brokerLogManagerService;

    private static final String BROKERS_LOG = "brokers.log";

    /**
     * Initialize the Broker Log (log that keeps track of the broker logs) and create a new broker logger for each connection.
     * @param api
     * @param dirPath
     * @param connectionNames
     */
    public static void initializeBrokerLogManager( GraphDatabaseAPI api, String dirPath, List<String> connectionNames )
    {
        BrokerLogManager.dirPath = dirPath;
        BrokerLogManager.graphDatabaseAPI = api;

        List<String> alreadyLoggedConnectionNames = new ArrayList<>(  );
        try
        {
            brokerLog = new File( dirPath + BROKERS_LOG );

            if (Files.exists(Paths.get(  brokerLog.getPath() ) ))
            {
                try(Stream<LogLine> logLineStream = streamLogLines())
                {
                    logLineStream.map( LogLine::getLogInfo ).forEach( logInfo -> {
                        alreadyLoggedConnectionNames.add( logInfo.getBrokerName() );
                    } );
                }
            }


            brokerLog.createNewFile();

            // Create logger for this manager
            brokerLogManagerService =
                    BrokerLogService.inLogsDirectory( graphDatabaseAPI.getDependencyResolver().resolveDependency( FileSystemAbstraction.class ),
                            new File( dirPath ), BROKERS_LOG );
        }
        catch ( Exception e )
        {
            new RuntimeException( "Unable to create 'brokers.log' log file. Exception: " + e.getMessage() );
        }

        nameToLogMap = new ConcurrentHashMap<>(  );

        connectionNames.stream().forEach( name -> {
            try
            {
                // Create and add loggers for each connection.
                nameToLogMap.put( name, new BrokerLogger( graphDatabaseAPI, dirPath, name ) );


                if(!alreadyLoggedConnectionNames.contains( name ))
                {
                    info( new LogLine.LogInfo( name, dirPath + name + ".log", 0L ) );
                }

            }
            catch ( Exception e )
            {
                new RuntimeException( "Unable to create '" + name + ".log' log file." );
            }
        } );
    }

    public static Stream<LogLine> streamLogLines() throws Exception
    {
        return Files.lines( Paths.get( brokerLog.getPath() ) ).map( LogLine::new );
    }

    /**
     * Takes in the connection name and returns the  corresponding LogInfo.
     * @param connectionName
     * @return
     * @throws Exception
     */
    public static Stream<LogLine.LogInfo> readBrokerLogLine(String connectionName) throws Exception
    {
        return Files.lines( Paths.get( brokerLog.getPath() ) ).map( LogLine::new ).map( LogLine::getLogInfo ).filter(
                logInfo -> logInfo.getBrokerName().equals( connectionName ) );
    }

    /**
     * Streams back the log lines.
     * @return
     * @throws Exception
     */
    public static Stream<LogLine.LogInfo> streamBrokerLogInfo( ) throws Exception
    {
        return Files.lines( Paths.get( brokerLog.getPath() ) ).map( LogLine::new ).map( LogLine::getLogInfo );
    }

    /**
     *
     * @param connectionName
     * @param messagePointer
     */
    public static void updateNextMessageToSend(String connectionName, Long messagePointer )
    {
        synchronized ( brokerLog )
        {

            String  tmpFileName = RandomStringUtils.randomAlphabetic( 5 );
            try
            {
                File tmpFile = File.createTempFile( tmpFileName, ".log", brokerLog.getParentFile() );
                tmpFile.deleteOnExit();
                try(FileOutputStream fileOutputStream = new FileOutputStream( tmpFile ); DataOutputStream dataOutputStream = new DataOutputStream( fileOutputStream ))
                {
                    tmpFileName = tmpFile.getName();
                    try(Stream<LogLine> logLineStream = streamLogLines())
                    {
                        logLineStream.forEach( logLine -> {
                            try
                            {
                                LogLine.LogInfo logInfo = logLine.getLogInfo();
                                if ( logInfo.getBrokerName().equals( connectionName ) )
                                {
                                    logLine.setLogInfo( new LogLine.LogInfo( logInfo.brokerName, logInfo.filePath, messagePointer ) );
                                }

                                dataOutputStream.write( logLine.getLogString().getBytes() );
                                dataOutputStream.writeChars( System.getProperty( "line.separator" ) );
                            }
                            catch ( Exception e )
                            {
                                throw new RuntimeException( "Failure to update the logInfo for connection '" + connectionName + "'." );
                            }
                        } );
                    }
                        org.apache.commons.io.FileUtils.copyFile( tmpFile, brokerLog );
                        org.apache.commons.io.FileUtils.deleteQuietly( tmpFile );

                }
            }
            catch ( Exception e )
            {
                // Make sure the tmp file got deleted.
                File tmpFile = new File( brokerLog.getParentFile() + "/"+ tmpFileName );
                if (tmpFile.exists())
                {
                    org.apache.commons.io.FileUtils.deleteQuietly( tmpFile );
                }
            }
        }
    }

    public static void resetBrokerLogger(String connectionName)
    {
        updateNextMessageToSend( connectionName, 0L );
        nameToLogMap.get( connectionName ).resetFile();
    }

    public static BrokerLogger getBrokerLogger(String connectionName)
    {
        return nameToLogMap.get( connectionName );
    }

    public static void info( LogLine.LogInfo logInfo ) throws Exception
    {
        info( OBJECT_MAPPER.writeValueAsString( logInfo ) );
    }

    public static void warn( LogLine.LogInfo logInfo ) throws Exception
    {
        warn( OBJECT_MAPPER.writeValueAsString( logInfo ) );
    }

    public static void debug( LogLine.LogInfo logInfo ) throws Exception
    {
        debug( OBJECT_MAPPER.writeValueAsString( logInfo ) );
    }

    public static void error( LogLine.LogInfo logInfo ) throws Exception
    {
        error( OBJECT_MAPPER.writeValueAsString( logInfo ) );
    }

    private static void info( String msg )
    {
        brokerLogManagerService.getInternalLogProvider().getLog( BROKERS_LOG ).info( msg );
    }

    private static void warn( String msg )
    {
        brokerLogManagerService.getInternalLogProvider().getLog( BROKERS_LOG ).warn( msg );
    }

    private static void debug( String msg )
    {
        brokerLogManagerService.getInternalLogProvider().getLog( BROKERS_LOG ).debug( msg );
    }

    private static void error( String msg )
    {
        brokerLogManagerService.getInternalLogProvider().getLog( BROKERS_LOG ).error( msg );
    }



    @JsonAutoDetect
    public static class LogLine
    {
        private String time;
        private String level;
        private String logName;
        private LogInfo logInfo;

        public LogLine()
        {
        }

        public LogLine( String time, String level, String logName, LogInfo logInfo )
        {
            this.time = time;
            this.level = level;
            this.logName = logName;
            this.logInfo = logInfo;
        }

        public LogLine( String logLine )
        {
            String[] splited = logLine.split( "\\s+", 5 );

            time = splited[0] + " " + splited[1];
            level = splited[2];
            logName = splited[3];
            try
            {
                logInfo = OBJECT_MAPPER.readValue( splited[4], LogInfo.class );
            }
            catch ( Exception e )
            {
                logInfo = new LogInfo(  );
            }
        }

        public String getLogString()
        {
            String result = "";

            result += time + " " + level + " " + logName + " ";
            try
            {
                result += OBJECT_MAPPER.writeValueAsString( logInfo );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( "Unable to write LogEntry as String" );
            }
            return result;
        }

        @JsonAutoDetect
        public static class LogInfo
        {
            private String brokerName;
            private String filePath;
            private Long nextMessageToSend;

            public LogInfo()
            {
            }

            public LogInfo( String brokerName, String filePath, Long nextMessageToSend )
            {
                this.brokerName = brokerName;
                this.filePath = filePath;
                this.nextMessageToSend = nextMessageToSend;
            }

            public String getBrokerName()
            {
                return brokerName;
            }

            public void setBrokerName( String brokerName )
            {
                this.brokerName = brokerName;
            }

            public String getFilePath()
            {
                return filePath;
            }

            public void setFilePath( String filePath )
            {
                this.filePath = filePath;
            }

            public Long getNextMessageToSend()
            {
                return nextMessageToSend;
            }

            public void setNextMessageToSend( Long nextMessageToSend )
            {
                this.nextMessageToSend = nextMessageToSend;
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

                LogInfo logInfo = (LogInfo) o;

                return new EqualsBuilder().append( brokerName, logInfo.brokerName ).append( filePath, logInfo.filePath ).append( nextMessageToSend, logInfo.nextMessageToSend ).isEquals();
            }

            @Override
            public int hashCode()
            {
                return new HashCodeBuilder( 17, 37 ).append( brokerName ).append( filePath ).append( nextMessageToSend ).toHashCode();
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

        public LogInfo getLogInfo()
        {
            return logInfo;
        }

        public void setLogInfo( LogInfo logInfo )
        {
            this.logInfo = logInfo;
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

            return new EqualsBuilder().append( time, logLine.time ).append( level, logLine.level ).append( logName, logLine.logName ).append( logInfo,
                    logLine.logInfo ).isEquals();
        }

        @Override
        public int hashCode()
        {
            return new HashCodeBuilder( 17, 37 ).append( time ).append( level ).append( logName ).append( logInfo ).toHashCode();
        }
    }
}
