package apoc.search;

import org.apache.lucene.queryparser.classic.ParseException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.fulltext.FulltextAdapter;
import org.neo4j.kernel.api.impl.fulltext.ScoreEntityIterator;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.util.FeatureToggles;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class NodeSearch
{

    private static final long INDEX_ONLINE_QUERY_TIMEOUT_SECONDS = FeatureToggles.getInteger(
            ParallelNodeSearch.class, "INDEX_ONLINE_QUERY_TIMEOUT_SECONDS", 30 );

    @Context
    public FulltextAdapter accessor;

    @Context
    public KernelTransaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public DependencyResolver resolver;

    @Description( "Query the given fulltext index. Returns the matching nodes and their lucene query score, ordered by score." )
    @Procedure( name = "apoc.index.fulltext.sortNodes", mode = READ )
    public Stream<NodeOutputCopy> querySortFulltextForNodes( @Name( "indexName" ) String name, @Name( "queryString" ) String query, @Name( "sortProperty") String sortProperty, @Name( "sortDirection") String sortDirectionString   )
            throws IndexNotFoundKernelException, IOException, ParseException, NoSuchFieldException, IllegalAccessException
    {
        IndexReference indexReference = getValidIndexReference( name );
        awaitOnline( indexReference );
        EntityType entityType = indexReference.schema().entityType();
        if ( entityType != EntityType.NODE )
        {
            throw new IllegalArgumentException( "The '" + name + "' index (" + indexReference + ") is an index on " + entityType +
                    ", so it cannot be queried for nodes." );
        }
        // Check that sortDirection is valid
        SortDirection sortDirection;
        if( sortDirectionString.equalsIgnoreCase( SortDirection.ASC.toString() ) )
        {
            sortDirection = SortDirection.ASC;
        }
        else if (sortDirectionString.equalsIgnoreCase( SortDirection.DESC.toString() ))
        {
            sortDirection = SortDirection.DESC;
        }
        else
        {
            throw new IOException( "Invalid input 'sortDirection' was '" + sortDirectionString + "'. Valid 'sortDirection' inputs are 'ASC' or 'DESC'." );
        }


        // Run Search
        ScoreEntityIterator resultIterator = accessor.query( tx, name, query );

        // Reflection
        Class<ScoreEntityIterator> scoreEntityIteratorClass = ScoreEntityIterator.class;
        Class<Object> clazz = (Class<Object>) scoreEntityIteratorClass.getDeclaredClasses()[0];

        List<NodeOutputCopy> nodeOutputCopyList = new ArrayList<>(  );
        Stream.Builder<NodeOutputCopy> resultBuilder = Stream.builder();

        Field entityIdField = clazz.getDeclaredField( "entityId" );
        entityIdField.setAccessible( true );

        Field scoreField = clazz.getDeclaredField( "score" );
        scoreField.setAccessible( true );

        // Have to collect into List so ScoreEntry can be casted to Object implicitly
        List<Object> implicitScoreEntry = resultIterator.stream().collect( Collectors.toList() );

        for ( Object o : implicitScoreEntry )
        {
            Long entityId = (Long) entityIdField.get( o );
            Float score = (Float) scoreField.get( o );
            nodeOutputCopyList.add( NodeOutputCopy.forExistingEntityOrNull( db, new ScoreEntryCopy( entityId, score ) ) );
        }

        System.out.println();
        System.out.println();
        System.out.println();

        for ( NodeOutputCopy nodeOutputCopy : nodeOutputCopyList )
        {
            nodeOutputCopy.node.getAllProperties().containsKey(  )
        }

//        return Arrays.stream( nodeOutputCopyList.toArray( new NodeOutputCopy[nodeOutputCopyList.size()] ) );
        return resultBuilder.build();

    }

    private IndexReference getValidIndexReference( @Name( "indexName" ) String name )
    {
        IndexReference indexReference = tx.schemaRead().indexGetForName( name );
        if ( indexReference == IndexReference.NO_INDEX )
        {
            throw new IllegalArgumentException( "There is no such fulltext schema index: " + name );
        }
        return indexReference;
    }

    private void awaitOnline( IndexReference indexReference ) throws IndexNotFoundKernelException
    {
        // We do the isAdded check on the transaction state first, because indexGetState will grab a schema read-lock, which can deadlock on the write-lock
        // held by the index populator. Also, if we index was created in this transaction, then we will never see it come online in this transaction anyway.
        // Indexes don't come online until the transaction that creates them has committed.
        if ( !((KernelTransactionImplementation)tx).txState().indexDiffSetsBySchema( indexReference.schema() ).isAdded( (IndexDescriptor) indexReference ) )
        {
            // If the index was not created in this transaction, then wait for it to come online before querying.
            Schema schema = db.schema();
            IndexDefinition index = schema.getIndexByName( indexReference.name() );
            schema.awaitIndexOnline( index, INDEX_ONLINE_QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS );
        }
        // If the index was created in this transaction, then we skip this check entirely.
        // We will get an exception later, when we try to get an IndexReader, so this is fine.
    }

    private <T> NodeOutputCopy f(ScoreEntryCopy result) throws Exception
    {
        try
        {
            return new NodeOutputCopy( db.getNodeById( result.entityId() ), result.score() );
        }
        catch ( NotFoundException ignore )
        {
            // This node was most likely deleted by a concurrent transaction, so we just ignore it.
            return null;
        }
    }

    private class ScoreEntryCopy
    {
        private final long entityId;
        private final float score;

        long entityId()
        {
            return entityId;
        }

        float score()
        {
            return score;
        }

        ScoreEntryCopy( long entityId, float score )
        {
            this.entityId = entityId;
            this.score = score;
        }

        @Override
        public String toString()
        {
            return "ScoreEntry[entityId=" + entityId + ", score=" + score + "]";
        }
    }

    private static final class NodeOutputCopy
    {
        public final Node node;
        public final double score;

        protected NodeOutputCopy( Node node, double score )
        {
            this.node = node;
            this.score = score;
        }

        public static NodeOutputCopy forExistingEntityOrNull( GraphDatabaseService db, ScoreEntryCopy result )
        {
            try
            {
                return new NodeOutputCopy( db.getNodeById( result.entityId() ), result.score() );
            }
            catch ( NotFoundException ignore )
            {
                // This node was most likely deleted by a concurrent transaction, so we just ignore it.
                return null;
            }
        }
    }

    private enum SortDirection{
        ASC,
        DESC
    }
}
