package apoc.cache;

import apoc.result.MapResult;
import apoc.result.ObjectResult;
import apoc.result.StringResult;
import org.apache.commons.lang3.RandomStringUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * @author ai
 * @since 02.01.19
 */
public class Dynamic
{

    @Context
    public GraphDatabaseAPI db;

    private static Map<String,ConcurrentHashMap<String,Object>> storage = new ConcurrentHashMap<>();

    @Procedure( "apoc.dynamic.open" )
    @Description( "apoc.dynamic.open() - Open a temporary map and return the generated key." )
    public Stream<StringResult> open( @Name( value = "keySize", defaultValue = "5" ) String keySize )
    {
        Integer keySizeInt = Integer.parseInt( keySize );
        String generatedKey = RandomStringUtils.randomAlphabetic( keySizeInt );

        while ( storage.containsKey( generatedKey ) )
        {
            generatedKey = RandomStringUtils.random( keySizeInt );
        }

        storage.put( generatedKey, new ConcurrentHashMap<>() );

        return Stream.of( new StringResult( generatedKey ) );
    }

    @Procedure( "apoc.dynamic.close" )
    @Description( "apoc.dynamic.close( superKey ) - Close a specified temporary map." )
    public Stream<MapResult> close( @Name( "superKey" ) String key )
    {

        if ( storage.containsKey( key ) )
        {
            storage.remove( key );
        }
        else
        {
            throw new RuntimeException( "The apoc.dynamic storage map does not contain the key '" + key + "'." );
        }
        Map<String,Object> result = new HashMap<>();
        result.put( "removed", key );
        return Stream.of( new MapResult( result ) );
    }

    @Procedure( "apoc.dynamic.get" )
    @Description( "apoc.dynamic.get( superKey, subKey ) - returns dynamically stored value from server storage" )
    public Stream<ObjectResult> get( @Name( "superKey" ) String superKey, @Name( "subKey" ) String subKey )
    {
        return Stream.of( new ObjectResult( retrieveSubmap( superKey ).get( subKey ) ) );
    }

    @Procedure( "apoc.dynamic.getAll" )
    @Description( "apoc.dynamic.getAll( superKey ) - Returns the entire temporary map by key" )
    public Stream<MapResult> getAll( @Name( "superKey" ) String superKey )
    {
        return Stream.of( new MapResult( retrieveSubmap( superKey ) ) );
    }

    @Procedure( "apoc.dynamic.set" )
    @Description( "apoc.dynamic.set( superKey, subKey, value ) - stores value under key for server livetime storage, returns previously stored or configured value" )
    public Stream<ObjectResult> set( @Name( "superKey" ) String superKey, @Name( "subKey" ) String subKey, @Name( "value" ) Object value )
    {
        ConcurrentHashMap<String,Object> superMap = retrieveSubmap( superKey );
        if ( value == null )
        {
            superMap.remove( subKey );
        }
        else
        {
            superMap.put( subKey, value );
        }
        return Stream.of( new ObjectResult( value ) );
    }

    private static ConcurrentHashMap<String,Object> retrieveSubmap( String superKey )
    {
        if ( storage.containsKey( superKey ) )
        {
            return storage.get( superKey );
        }
        else
        {
            throw new RuntimeException( "The apoc.dynamic storage map does not contain the key '" + superKey + "'." );
        }
    }

    public static void clear()
    {

        storage.clear();
    }
}
