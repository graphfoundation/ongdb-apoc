package apoc.trigger;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

/**
 * @author alexanderiudice
 * @since 04.02.2020
 */
public class TransactionTest
{
    private GraphDatabaseService db;
    private long start;

    @Before
    public void setUp() throws Exception
    {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase();
        start = System.currentTimeMillis();
        TestUtil.registerProcedure( db, Trigger.class );
    }

    @After
    public void tearDown()
    {
        if ( db != null )
        {
            db.shutdown();
        }
    }

    @Test
    public void testTransactionIdConsistency() throws Exception
    {
        String querySchema = "CREATE CONSTRAINT ON (n:Person) ASSERT n.name IS UNIQUE";
        String queryWriteA = "CREATE (n:Person) SET n.name = 'John' RETURN n";
        String queryWriteB = "CREATE (n:Person) SET n.name = 'Jane' RETURN n";
        String queryRead = "MATCH (n:Person) WHERE n.name = 'John' RETURN n";

        // Set up schema
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( querySchema );
            tx.success();
        }

        // Write to increase the txId
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( queryWriteA );
            tx.success();
            // txId starts off at 5, will increase when this transaction is commited/closed.
            assertEquals( 5, ((GraphDatabaseFacade) db).kernelTransaction().lastTransactionIdWhenStarted() );
        }
        // Assert txId is incremented by 1
        try ( Transaction tx = db.beginTx() )
        {
            tx.success();
            assertEquals( 6, ((GraphDatabaseFacade) db).kernelTransaction().lastTransactionIdWhenStarted() );
        }


        // Three bad rolled-backed write transactions
        for ( int i = 0; i < 3; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.execute( queryWriteA );
                Assert.fail( "Should not get here. This write should fail due to constraint." );
            }
            catch ( Exception e )
            {

            }
        }
        // Assert txId is unchanged from rollbacks
        try ( Transaction tx = db.beginTx() )
        {
            tx.success();
            assertEquals( 6, ((GraphDatabaseFacade) db).kernelTransaction().lastTransactionIdWhenStarted() );
        }

        // Read txId
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( queryRead );
            tx.success();
        }
        // Assert txId is unchanged from read transcation
        try ( Transaction tx = db.beginTx() )
        {
            tx.success();
            assertEquals( 6, ((GraphDatabaseFacade) db).kernelTransaction().lastTransactionIdWhenStarted() );
        }

        // Write txId
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( queryWriteB );
            tx.success();
            assertEquals( 6, ((GraphDatabaseFacade) db).kernelTransaction().lastTransactionIdWhenStarted() );
        }
        // assert it has been incremented by 1
        try ( Transaction tx = db.beginTx() )
        {
            tx.success();
            assertEquals( 7, ((GraphDatabaseFacade) db).kernelTransaction().lastTransactionIdWhenStarted() );
        }
    }
}
