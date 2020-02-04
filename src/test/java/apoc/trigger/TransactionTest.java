package apoc.trigger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static org.junit.Assert.assertEquals;

/**
 * @author alexanderiudice
 * @since 04.02.2020
 */
public class TransactionTest
{
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception
    {
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
            db.executeTransactionally( querySchema );
            tx.commit();
        }

        // Write to increase the txId - it starts at 8 for some reason.
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( queryWriteA );
            // txId starts off at 5, will increase when this transaction is commited/closed.
            assertEquals( 8, ((InternalTransaction) tx).kernelTransaction().lastTransactionIdWhenStarted() );
            tx.commit();
        }

        // Assert txId is incremented by 1
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 9, ((InternalTransaction) tx).kernelTransaction().lastTransactionIdWhenStarted() );
            tx.commit();
        }

        // Three bad rolled-backed write transactions
        for ( int i = 0; i < 3; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.execute( queryWriteA );
                Assert.fail( "Should not get here. This write should fail due to constraint." );
            }
            catch ( Exception e )
            {
            }
        }
        // Assert txId is unchanged from rollbacks
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 9, ((InternalTransaction) tx).kernelTransaction().lastTransactionIdWhenStarted() );
            tx.commit();
        }

        // Read txId
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( queryRead );
            tx.commit();
        }
        // Assert txId is unchanged from read transcation
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 9, ((InternalTransaction) tx).kernelTransaction().lastTransactionIdWhenStarted() );
            tx.commit();
        }

        // Write txId
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( queryWriteB );
            assertEquals( 9, ((InternalTransaction) tx).kernelTransaction().lastTransactionIdWhenStarted() );
            tx.commit();
        }
        // assert it has been incremented by 1
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 10, ((InternalTransaction) tx).kernelTransaction().lastTransactionIdWhenStarted() );
            tx.commit();
        }
    }
}
