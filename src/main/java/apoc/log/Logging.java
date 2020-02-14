package apoc.log;

import apoc.Pools;
import apoc.ThreadPoolExecutorLogger;
import apoc.result.MapResult;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

/**
 * @author bradnussbaum
 * @since 2017.07.28
 */
public class Logging
{

    @Context
    public Log log;

    @Procedure
    @Description( "apoc.log.error(message, params) - logs error message" )
    public void error( @Name( "message" ) String message,
            @Name( value = "params", defaultValue = "{}" ) Map<String,Object> params )
    {
        log.error( message, params );
    }

    @Procedure
    @Description( "apoc.log.warn(message, params) - logs warn message" )
    public void warn( @Name( "message" ) String message,
            @Name( value = "params", defaultValue = "{}" ) Map<String,Object> params )
    {
        log.warn( message, params );
    }

    @Procedure
    @Description( "apoc.log.info(message, params) - logs info message" )
    public void info( @Name( "message" ) String message,
            @Name( value = "params", defaultValue = "{}" ) Map<String,Object> params )
    {
        log.info( message, params );
    }

    @Procedure
    @Description( "apoc.log.debug(message, params) - logs debug message" )
    public void debug( @Name( "message" ) String message,
            @Name( value = "params", defaultValue = "{}" ) Map<String,Object> params )
    {
        log.debug( message, params );
    }

    @Procedure
    @Description( "apoc.log.threadPools() - logs threading info." )
    public Stream<MapResult> threadPools()
    {
        Map<String,Object> singleInfo = ((ThreadPoolExecutorLogger) Pools.SINGLE).getInfo();
        Map<String,Object> defaultInfo = ((ThreadPoolExecutorLogger) Pools.DEFAULT).getInfo();
        Map<String,Object> brokerInfo = ((ThreadPoolExecutorLogger) Pools.BROKER).getInfo();

        return Stream.of( new MapResult( singleInfo ), new MapResult( defaultInfo ), new MapResult( brokerInfo ) );
    }
}
