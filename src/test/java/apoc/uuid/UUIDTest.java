package apoc.uuid;

import apoc.create.Create;
import apoc.periodic.Periodic;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author ab-larus
 * @since 05.09.18
 */
public class UUIDTest {

    private GraphDatabaseService db;

    private static final String UUID_TEST_REGEXP = "^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$";

    @Before
    public void setUp() throws Exception {
        db = TestUtil.apocGraphDatabaseBuilder()
                .setConfig("apoc.uuid.enabled", "true")
                .setConfig("dbms.security.auth_enabled", "true")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, Uuid.class);
        TestUtil.registerProcedure(db, Create.class);
        TestUtil.registerProcedure(db, Periodic.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testUUID() {
        // given
        db.execute("CREATE CONSTRAINT ON (p:Person) ASSERT p.uuid IS UNIQUE").close();
        db.execute("CALL apoc.uuid.install('Person') YIELD label RETURN label").close();

        // when
        db.execute("CREATE (p:Person{name:'Daniel'})-[:WORK]->(c:Company{name:'Neo4j'})").close();

        // then
        try (Transaction tx = db.beginTx()) {
            Node company = (Node) db.execute("MATCH (c:Company) return c").next().get("c");
            assertTrue(!company.hasProperty("uuid"));
            Node person = (Node) db.execute("MATCH (p:Person) return p").next().get("p");
            assertTrue(person.getAllProperties().containsKey("uuid"));

            assertTrue(person.getAllProperties().get("uuid").toString().matches(UUID_TEST_REGEXP));
            tx.success();
        }
    }

    @Test
    public void testUUIDWithoutRemovedUuid() {
        // given
        db.execute("CREATE CONSTRAINT ON (test:Test) ASSERT test.uuid IS UNIQUE").close();
        db.execute("CALL apoc.uuid.install('Test') YIELD label RETURN label").close();

        // when
        db.execute("CREATE (n:Test {name:'test', uuid:'dab404ee-391d-11e9-b210-d663bd873d93'})").close(); // Create the uuid manually and except is the same after the trigger

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) db.execute("MATCH (n:Test) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertEquals("dab404ee-391d-11e9-b210-d663bd873d93", n.getProperty("uuid")); // Check if the uuid if the same when created
            tx.success();
        }
    }

    @Test
    public void testUUIDSetUuidToEmptyAndRestore() {
        // given
        db.execute("CREATE CONSTRAINT ON (test:Test) ASSERT test.uuid IS UNIQUE").close();
        db.execute("CALL apoc.uuid.install('Test') YIELD label RETURN label").close();
        db.execute("CREATE (n:Test {name:'test', uuid:'dab404ee-391d-11e9-b210-d663bd873d93'})").close();

        // when
        db.execute("MATCH (t:Test) SET t.uuid = ''").close();

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) db.execute("MATCH (n:Test) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertEquals("dab404ee-391d-11e9-b210-d663bd873d93", n.getProperty("uuid"));
            tx.success();
        }
    }

    @Test
    public void testUUIDDeleteUuidAndRestore() {
        // given
        db.execute("CREATE CONSTRAINT ON (test:Test) ASSERT test.uuid IS UNIQUE").close();
        db.execute("CALL apoc.uuid.install('Test') YIELD label RETURN label").close();
        db.execute("CREATE (n:Test {name:'test', uuid:'dab404ee-391d-11e9-b210-d663bd873d93'})").close();

        // when
        db.execute("MATCH (t:Test) remove t.uuid").close();

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) db.execute("MATCH (n:Test) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertEquals("dab404ee-391d-11e9-b210-d663bd873d93", n.getProperty("uuid"));
            tx.success();
        }
    }

    @Test
    public void testUUIDSetUuidToEmpty() {
        // given
        db.execute("CREATE CONSTRAINT ON (test:Test) ASSERT test.uuid IS UNIQUE").close();
        db.execute("CALL apoc.uuid.install('Test') YIELD label RETURN label").close();
        db.execute("CREATE (n:Test:Empty {name:'empty'})").close();

        // when
        db.execute("MATCH (t:Test:Empty) SET t.uuid = ''").close();

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) db.execute("MATCH (n:Empty) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertTrue(n.getAllProperties().get("uuid").toString().matches("^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"));
            tx.success();
        }
    }

    @Test
    public void testUUIDList() {
        // given
        db.execute("CREATE CONSTRAINT ON (bar:Bar) ASSERT bar.uuid IS UNIQUE").close();

        // when
        db.execute("CALL apoc.uuid.install('Bar') YIELD label RETURN label").close();

        // then
        TestUtil.testCall(db, "CALL apoc.uuid.list()",
                (row) -> assertResult(row, "Bar", true,
                        Util.map("addToExistingNodes", true, "uuidProperty", "uuid")));
    }

    @Test
    public void testUUIDListAddToExistingNodes() {
        // given
        db.execute("CREATE CONSTRAINT ON (bar:Bar) ASSERT bar.uuid IS UNIQUE").close();
        db.execute("UNWIND Range(1,10) as i CREATE(bar:Bar{id: i})").close();

        // when
        db.execute("CALL apoc.uuid.install('Bar')").close();

        // then
        List<String> uuidList = db.execute("MATCH (n:Bar) RETURN n.uuid AS uuid").<String>columnAs("uuid")
                .stream().collect(Collectors.toList());
        assertEquals(10, uuidList.size());
        assertTrue(uuidList.stream().allMatch(uuid -> uuid.matches(UUID_TEST_REGEXP)));
    }

    @Test
    public void testAddRemoveUuid() {
        // given
        db.execute("CREATE CONSTRAINT ON (test:Test) ASSERT test.foo IS UNIQUE").close();

        // when
        db.execute("CALL apoc.uuid.install('Test', {uuidProperty: 'foo'}) YIELD label RETURN label").close();

        // then
        TestUtil.testCall(db, "CALL apoc.uuid.list()",
                (row) -> assertResult(row, "Test", true,
                        Util.map("addToExistingNodes", true, "uuidProperty", "foo")));
        TestUtil.testCall(db, "CALL apoc.uuid.remove('Test')",
                (row) -> assertResult(row, "Test", false,
                        Util.map("addToExistingNodes", true, "uuidProperty", "foo")));
    }

    @Test
    public void testNotAddToExistingNodes() {
        // given
        db.execute("CREATE (d:User {name:'Daniel'})-[:WORK]->(l:Company {name:'Neo4j'})").close();

        // when
        db.execute("CREATE CONSTRAINT ON (user:User) ASSERT user.uuid IS UNIQUE").close();
        db.execute("CALL apoc.uuid.install('User', {addToExistingNodes: false}) YIELD label RETURN label").close();

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) db.execute("MATCH (user:User) return user").next().get("user");
            assertFalse(n.getAllProperties().containsKey("uuid"));
            tx.success();
        }
    }

    @Test
    public void testAddToExistingNodes() {
        // given
        db.execute("CREATE (d:Person {name:'Daniel'})-[:WORK]->(l:Company {name:'Neo4j'})").close();

        // when
        db.execute("CREATE CONSTRAINT ON (person:Person) ASSERT person.uuid IS UNIQUE").close();
        db.execute("CALL apoc.uuid.install('Person') YIELD label RETURN label").close();

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) db.execute("MATCH (person:Person) return person").next().get("person");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertTrue(n.getAllProperties().get("uuid").toString().matches("^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"));
            tx.success();
        }
    }

    @Test
    public void testAddToExistingNodesBatchResult() {
        // given
        db.execute("CREATE (d:Person {name:'Daniel'})-[:WORK]->(l:Company {name:'Neo4j'})");

        // when
        db.execute("CREATE CONSTRAINT ON (person:Person) ASSERT person.uuid IS UNIQUE");

        // then
        try (Transaction tx = db.beginTx()) {
            long total = (Long) db.execute(
                    "CALL apoc.uuid.install('Person') YIELD label, installed, properties, batchComputationResult " +
                            "RETURN batchComputationResult.total as total")
                    .next()
                    .get("total");
            assertEquals(1, total);
            tx.success();
        }
    }

    @Test
    public void testRemoveAllUuid() {
        // given
        db.execute("CREATE CONSTRAINT ON (test:Test) ASSERT test.foo IS UNIQUE").close();
        db.execute("CREATE CONSTRAINT ON (bar:Bar) ASSERT bar.uuid IS UNIQUE").close();
        db.execute("CALL apoc.uuid.install('Bar') YIELD label RETURN label").close();
        db.execute("CALL apoc.uuid.install('Test', {addToExistingNodes: false, uuidProperty: 'foo'}) YIELD label RETURN label");

        // when
        TestUtil.testResult(db, "CALL apoc.uuid.removeAll()",
                (result) -> {
                    // then
                    Map<String, Object> row = result.next();
                    assertResult(row, "Test", false,
                            Util.map("addToExistingNodes", false, "uuidProperty", "foo"));
                    row = result.next();
                    assertResult(row, "Bar", false,
                            Util.map("addToExistingNodes", true, "uuidProperty", "uuid"));
                });
    }

    @Test(expected = RuntimeException.class)
    public void testAddWithError() {
        try {
            // when
            db.execute("CALL apoc.uuid.install('Wrong') YIELD label RETURN label").close();
        } catch (RuntimeException e) {
            // then
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals("No constraint found for label: Wrong, please add the constraint with the following : `CREATE CONSTRAINT ON (wrong:Wrong) ASSERT wrong.uuid IS UNIQUE`", except.getMessage());
            throw e;
        }
    }

    @Test(expected = RuntimeException.class)
    public void testAddWithErrorAndCustomField() {
        try {
            // when
            db.execute("CALL apoc.uuid.install('Wrong', {uuidProperty: 'foo'}) YIELD label RETURN label").close();
        } catch (RuntimeException e) {
            // then
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals("No constraint found for label: Wrong, please add the constraint with the following : `CREATE CONSTRAINT ON (wrong:Wrong) ASSERT wrong.foo IS UNIQUE`", except.getMessage());
            throw e;
        }
    }

    private void assertResult(Map<String, Object> row, String labels, boolean installed, Map<String, Object> conf) {
        assertEquals(labels, row.get("label"));
        assertEquals(installed, row.get("installed"));
        assertEquals(conf, row.get("properties"));
    }

}