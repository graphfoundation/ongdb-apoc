package apoc.cache;

import apoc.convert.Convert;
import apoc.util.TestUtil;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author ai
 * @since 03.01.19
 */
public class DynamicTest
{
    public static final String TEST_STRING = "testString";
    public static Long TEST_LONG = 1L;

    private static final List<Long> FIB_LIST = Arrays.asList(1L, 3L, 6L, 10L, 15L, 21L, 28L, 36L, 45L, 55L);

    private static final String SUB_MAP_A = "subMapA";
    private static final String FIB_MAP = "fibSeq";

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public  void setUp() throws Exception {
        TestUtil.registerProcedure(db, Dynamic.class);
        TestUtil.registerProcedure(db, Convert.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testDynamicMap_basicString() {

        String query =
                " CALL apoc.dynamic.open() YIELD value AS key" +
                        " CALL apoc.dynamic.set(key, $subMapA, $testString) YIELD value AS setMap" +
                        " CALL apoc.dynamic.get(key, $subMapA) YIELD value AS storedValue" +
                        " CALL apoc.dynamic.close(key) YIELD value AS closed" +
                        " RETURN storedValue";

        TestUtil.testCall( db, query, ImmutableMap.of( SUB_MAP_A, SUB_MAP_A, TEST_STRING, TEST_STRING ), ( row ) -> {
            assertEquals( TEST_STRING, (row.get( "storedValue" )) );
        } );
    }


    @Test
    public void testDynamicMap_openSize() {

        String query =
                " CALL apoc.dynamic.open('10') YIELD value AS key" +
                        " CALL apoc.dynamic.close(key) YIELD value AS closed" +
                        " RETURN size(key) AS keySize";

        TestUtil.testCall( db, query, ( row ) -> {
            assertEquals( 10L, row.get( "keySize" ) );
        } );
    }


    @Test
    public void testDynamicMap_referenceAfterCloseShouldFail() {

        String query =
                " CALL apoc.dynamic.open() YIELD value AS key" +
                        " CALL apoc.dynamic.close(key) YIELD value AS closed" +
                        " CALL apoc.dynamic.set(key, 'submap', 'testString') YIELD value AS setMap" +
                        " RETURN 1";

        try
        {
            TestUtil.testCall(db, query, (row) -> { });
        }
        catch ( RuntimeException e )
        {
            return;
        }
        assert false;
    }


    @Test
    public void testDynamicMap_fibonacci() {

        String query =
                " CALL apoc.dynamic.open() YIELD value AS key" +
                        " CALL apoc.dynamic.set(key, $fibSeq, [$testLong]) YIELD value AS setMap" +
                        " WITH key, range(2,10) AS ls" +
                        " UNWIND ls AS l" +
                            " WITH key, l" +
                            " CALL apoc.dynamic.get(key, $fibSeq) YIELD value AS fib" +
                            " WITH key, l, apoc.convert.toList(fib) AS fibList" +
                            " CALL apoc.dynamic.set(key, $fibSeq, fibList + [l + last(fibList)]) YIELD value" +
                        " WITH key, collect(l) AS ls" +
                        " CALL apoc.dynamic.get(key, $fibSeq) YIELD value AS result" +
                        " CALL apoc.dynamic.close(key) YIELD value AS closed" +
                        " WITH apoc.convert.toList(result) AS resultList" +
                        " RETURN resultList";

        TestUtil.testCall( db, query, ImmutableMap.of( FIB_MAP, FIB_MAP, "testLong", TEST_LONG ), ( row ) -> {
            assertEquals( FIB_LIST, (row.get( "resultList" )) );
        } );
    }
}
