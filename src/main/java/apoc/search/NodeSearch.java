package apoc.search;

import org.apache.lucene.queryparser.classic.ParseException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.fulltext.FulltextAdapter;
import org.neo4j.kernel.api.impl.fulltext.FulltextProcedures;
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
import java.util.Objects;
import java.util.concurrent.TimeUnit;
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
    public Stream<FulltextProcedures.NodeOutput> queryFulltextForNodes( @Name( "indexName" ) String name, @Name( "queryString" ) String query )
            throws IndexNotFoundKernelException, IOException, ParseException
    {
        IndexReference indexReference = getValidIndexReference( name );
        awaitOnline( indexReference );
        EntityType entityType = indexReference.schema().entityType();
        if ( entityType != EntityType.NODE )
        {
            throw new IllegalArgumentException( "The '" + name + "' index (" + indexReference + ") is an index on " + entityType +
                    ", so it cannot be queried for nodes." );
        }
        ScoreEntityIterator resultIterator = accessor.query( tx, name, query );
        return null;
//        return resultIterator.stream()
//                .map( result -> FulltextProcedures.NodeOutput.forExistingEntityOrNull( db, result ) )
//                .filter( Objects::nonNull );
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
}
