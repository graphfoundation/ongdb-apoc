package apoc.trigger;

import apoc.cache.Static;
import apoc.cypher.Cypher;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.ApocSettings.apoc_trigger_enabled;
import static apoc.trigger.TransactionDataMap.ASSIGNED_LABELS;
import static apoc.trigger.TransactionDataMap.ASSIGNED_NODE_PROPERTIES;
import static apoc.trigger.TransactionDataMap.ASSIGNED_RELATIONSHIP_PROPERTIES;
import static apoc.trigger.TransactionDataMap.COMMIT_TIME;
import static apoc.trigger.TransactionDataMap.CREATED_NODES;
import static apoc.trigger.TransactionDataMap.CREATED_RELATIONSHIPS;
import static apoc.trigger.TransactionDataMap.DELETED_NODES;
import static apoc.trigger.TransactionDataMap.DELETED_RELATIONSHIPS;
import static apoc.trigger.TransactionDataMap.REMOVED_LABELS;
import static apoc.trigger.TransactionDataMap.REMOVED_NODE_PROPERTIES;
import static apoc.trigger.TransactionDataMap.REMOVED_RELATIONSHIP_PROPERTIES;
import static apoc.trigger.TransactionDataMap.TRANSACTION_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TriggerDataTest
{
    private long start;

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(apoc_trigger_enabled, true);  // need to use settings here, apocConfig().setProperty in `setUp` is too late

    @Before
    public void setUp() {
        start = System.currentTimeMillis();
        TestUtil.registerProcedure(db, Trigger.class);
        TestUtil.registerProcedure(db, Static.class);
    }

    @After
    public void tearDown() {
        if (db!=null) db.shutdown();
    }

    @Test
    public void testTriggerData_TransactionId() {
        db.executeTransactionally( "CALL apoc.trigger.add('test','WITH $createdNodes AS createdNodes, $txData AS txData UNWIND $createdNodes AS n SET n.testProp = txData." + TRANSACTION_ID + "',{phase: 'after'}, { params: {uidKeys: ['uid']}})");
        db.executeTransactionally("CREATE (f:Foo {name:'Michael'})");
        TestUtil.testCall(db, "MATCH (f:Foo) RETURN f", (row) -> {
            assertTrue( ((Node) row.get( "f" )).hasProperty( "testProp" ) );
            assertNotEquals( -1L, ((Node)row.get("f")).getProperty( "testProp") );
        });
    }

    @Test
    public void testTriggerData_CommitTime() {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $createdNodes AS createdNodes, $txData AS txData UNWIND $createdNodes AS n SET n.testProp = txData." + COMMIT_TIME + "',{phase: 'after'}, { params: {uidKeys: ['uid']}})");
        db.executeTransactionally("CREATE (f:Foo {name:'Michael'})");
        TestUtil.testCall(db, "MATCH (f:Foo) RETURN f", (row) -> {
            assertTrue( ((Node) row.get( "f" )).hasProperty( "testProp" ) );
            assertNotEquals( -1L, ((Node)row.get("f")).getProperty( "testProp") );
        });
    }

    private void testTriggerData_CreatedNodes( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $createdNodes AS createdNodes, $txData AS txData UNWIND $createdNodes AS n SET n.testProp = keys(txData." + CREATED_NODES + ")[0]',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");
        db.executeTransactionally("CREATE (f:Foo {name:'Michael', uid: 'uid-node-1'})");
        TestUtil.testCall(db, "MATCH (f:Foo) RETURN f", (row) -> {
            assertTrue( ((Node) row.get( "f" )).hasProperty( "testProp" ) );
            assertEquals( "`Foo`:`uid`:`uid-node-1`", ((Node)row.get("f")).getProperty( "testProp") );
        });
    }

    @Test
    public void testTriggerData_CreatedNodes_before() {
        testTriggerData_CreatedNodes( "before" );
    }

    @Test
    public void testTriggerData_CreatedNodes_after() {
        testTriggerData_CreatedNodes( "after" );
    }

    private void testTriggerData_DeletedNodes( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $txData  AS txData UNWIND keys(txData." + DELETED_NODES + ") AS key CALL apoc.static.set(\\'testProp\\', key) YIELD value RETURN 1',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");
        db.executeTransactionally("CREATE (f:Foo {name:'Michael', uid: 'uid-node-1'})");
        db.executeTransactionally("MATCH (f:Foo) WHERE f.uid = 'uid-node-1' DELETE f");
        TestUtil.testCall(db, "CALL apoc.static.get('testProp') YIELD value RETURN value", (row) -> assertEquals("`Foo`:`uid`:`uid-node-1`", row.get("value") ) );
    }

    @Test
    public void testTriggerData_DeletedNodes_before() {
        testTriggerData_DeletedNodes( "before" );
    }

    @Test
    public void testTriggerData_DeletedNodes_after() {
        testTriggerData_DeletedNodes( "after" );
    }

    private void testTriggerData_CreatedRelationships( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $createdRelationships AS createdRelationships, $txData AS txData UNWIND $createdRelationships AS r SET r.testProp = keys(txData." + CREATED_RELATIONSHIPS + ")[0]',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");
        db.executeTransactionally("CREATE (f:Foo {name:'Michael'})-[r:BAR {uid:'uid-rel-1'}]->(g:Foo {name:'John'})");
        TestUtil.testCall(db, "MATCH (f:Foo)-[r:BAR]->(g:Foo) RETURN r", (row) -> {
            assertTrue( ((Relationship) row.get( "r" )).hasProperty( "testProp" ) );
            assertEquals( "`BAR`:`uid`:`uid-rel-1`", ((Relationship)row.get("r")).getProperty( "testProp") );
        });
    }

    @Test
    public void testTriggerData_CreatedRelationships_before() {
        testTriggerData_CreatedRelationships( "before" );
    }

    @Test
    public void testTriggerData_CreatedRelationships_after() {
        testTriggerData_CreatedRelationships( "after" );
    }

    private void testTriggerData_DeletedRelationships( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $txData  AS txData UNWIND keys(txData." + DELETED_RELATIONSHIPS + ") AS key CALL apoc.static.set(\\'testProp\\', key) YIELD value RETURN 1',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");
        db.executeTransactionally("CREATE (f:Foo {name:'Michael'})-[r:BAR {uid:'uid-rel-1'}]->(g:Foo {name:'John'})");
        db.executeTransactionally("MATCH (f:Foo)-[r:BAR]-(g:Foo) DELETE r");
        TestUtil.testCall(db, "CALL apoc.static.get('testProp') YIELD value RETURN value", (row) -> assertEquals("`BAR`:`uid`:`uid-rel-1`", row.get("value") ) );
    }

    @Test
    public void testTriggerData_DeletedRelationships_before() {
        testTriggerData_DeletedRelationships( "before" );
    }

    @Test
    public void testTriggerData_DeletedRelationships_after() {
        testTriggerData_DeletedRelationships( "after" );
    }

    private void testTriggerData_AssignedLabels_ByLabel( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $txData AS txData CALL apoc.static.set(\\'testProp\\', txData." + ASSIGNED_LABELS + ".byLabel.Foo[0].nodeUid) YIELD value RETURN 1',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");
        db.executeTransactionally("CREATE (f:Foo {name:'Michael', uid: 'uid-node-1'})");
        TestUtil.testCall(db, "CALL apoc.static.get('testProp') YIELD value RETURN value", (row) -> assertEquals("`Foo`:`uid`:`uid-node-1`", row.get("value") ) );
    }

    @Test
    public void testTriggerData_AssignedLabels_ByLabel_before() {
        testTriggerData_AssignedLabels_ByLabel( "before" );
    }

    @Test
    public void testTriggerData_AssignedLabels_ByLabel_after() {
        testTriggerData_AssignedLabels_ByLabel( "after" );
    }

    private void testTriggerData_AssignedLabels_ByUid( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $txData AS txData CALL apoc.static.set(\\'testProp\\', keys(txData." + ASSIGNED_LABELS + ".byUid)[0]) YIELD value RETURN 1',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");
        db.executeTransactionally("CREATE (f:Foo {name:'Michael', uid: 'uid-node-1'})");
        TestUtil.testCall(db, "CALL apoc.static.get('testProp') YIELD value RETURN value", (row) -> assertEquals("`Foo`:`uid`:`uid-node-1`", row.get("value") ) );
    }

    @Test
    public void testTriggerData_AssignedLabels_ByUid_before() {
        testTriggerData_AssignedLabels_ByUid( "before" );
    }

    @Test
    public void testTriggerData_AssignedLabels_ByUid_after() {
        testTriggerData_AssignedLabels_ByUid( "after" );
    }

    private void testTriggerData_RemovedLabels_ByLabel( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $txData AS txData CALL apoc.static.set(\\'testProp\\', txData." + REMOVED_LABELS + ".byLabel.Foo[0].nodeUid) YIELD value RETURN 1',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");
        db.executeTransactionally("CREATE (f:Foo {name:'Michael', uid: 'uid-node-1'})");
        db.executeTransactionally("MATCH (f:Foo) WHERE f.uid = 'uid-node-1' DELETE f");
        TestUtil.testCall(db, "CALL apoc.static.get('testProp') YIELD value RETURN value", (row) -> assertEquals("`Foo`:`uid`:`uid-node-1`", row.get("value") ) );
    }

    @Test
    public void testTriggerData_RemovedLabels_ByLabel_before() {
        testTriggerData_RemovedLabels_ByLabel( "before" );
    }

    @Test
    public void testTriggerData_RemovedLabels_ByLabel_after() {
        testTriggerData_RemovedLabels_ByLabel( "after" );
    }

    private void testTriggerData_RemovedLabels_ByUid( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $txData AS txData CALL apoc.static.set(\\'testProp\\', keys(txData." + REMOVED_LABELS + ".byUid)[0]) YIELD value RETURN 1',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");
        db.executeTransactionally("CREATE (f:Foo {name:'Michael', uid: 'uid-node-1'})");
        db.executeTransactionally("MATCH (f:Foo) WHERE f.uid = 'uid-node-1' DELETE f");
        TestUtil.testCall(db, "CALL apoc.static.get('testProp') YIELD value RETURN value", (row) -> assertEquals("`Foo`:`uid`:`uid-node-1`", row.get("value") ) );
    }
    
    @Test
    public void testTriggerData_RemovedLabels_ByUid_before() {
        testTriggerData_RemovedLabels_ByUid( "before" );
    }

    @Test
    public void testTriggerData_RemovedLabels_ByUid_after() {
        testTriggerData_RemovedLabels_ByUid( "after" );
    }

    private void testTriggerData_AssignedNodeProperties_ByLabel( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $txData AS txData WITH txData, txData." + ASSIGNED_NODE_PROPERTIES + ".byLabel[\\'Foo\\'][\\'`Foo`:`uid`:`uid-node-1`\\'][0] AS props" +
                " WITH props.key AS keyProp, props.value AS valueProp" +
                " CALL apoc.static.set(\\'keyProp\\', keyProp) YIELD value" +
                " WITH valueProp, value AS v1" +
                " CALL apoc.static.set(\\'valueProp\\', valueProp) YIELD value" +
                " RETURN 1',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");

        db.executeTransactionally("CREATE (f:Foo {name:'Michael', uid: 'uid-node-1'})");
        db.executeTransactionally("MATCH (f:Foo {name:'Michael', uid: 'uid-node-1'}) SET f.color = 'blue'");

        TestUtil.testCall(db, "CALL apoc.static.get('keyProp') YIELD value RETURN value", (row) -> assertEquals("color", row.get("value") ) );
        TestUtil.testCall(db, "CALL apoc.static.get('valueProp') YIELD value RETURN value", (row) -> assertEquals("blue", row.get("value") ) );
    }

    @Test
    public void testTriggerData_AssignedNodeProperties_ByLabel_before() {
        testTriggerData_AssignedNodeProperties_ByLabel( "before" );
    }

    @Test
    public void testTriggerData_AssignedNodeProperties_ByLabel_after() {
        testTriggerData_AssignedNodeProperties_ByLabel( "after" );
    }

    private void testTriggerData_AssignedNodeProperties_ByKey( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $txData AS txData WITH txData, txData." + ASSIGNED_NODE_PROPERTIES + ".byKey.color[0] AS uidProp" +
                " CALL apoc.static.set(\\'uidProp\\', uidProp) YIELD value" +
                " RETURN 1',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");

        db.executeTransactionally("CREATE (f:Foo {name:'Michael', uid: 'uid-node-1'})");
        db.executeTransactionally("MATCH (f:Foo {name:'Michael', uid: 'uid-node-1'}) SET f.color = 'blue'");

        TestUtil.testCall(db, "CALL apoc.static.get('uidProp') YIELD value RETURN value", (row) -> assertEquals("`Foo`:`uid`:`uid-node-1`", row.get("value") ) );
    }
    
    @Test
    public void testTriggerData_AssignedNodeProperties_ByKey_before() {
        testTriggerData_AssignedNodeProperties_ByKey( "before" );
    }

    @Test
    public void testTriggerData_AssignedNodeProperties_ByKey_after() {
        testTriggerData_AssignedNodeProperties_ByKey( "after" );
    }

    private void testTriggerData_AssignedNodeProperties_ByUid( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $txData AS txData WITH txData, txData." + ASSIGNED_NODE_PROPERTIES + ".byUid[\\'`Foo`:`uid`:`uid-node-1`\\'][0] AS props" +
                " WITH props.key AS keyProp, props.value AS valueProp" +
                " CALL apoc.static.set(\\'keyProp\\', keyProp) YIELD value" +
                " WITH valueProp, value AS v1" +
                " CALL apoc.static.set(\\'valueProp\\', valueProp) YIELD value" +
                " RETURN 1',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");

        db.executeTransactionally("CREATE (f:Foo {name:'Michael', uid: 'uid-node-1'})");
        db.executeTransactionally("MATCH (f:Foo {name:'Michael', uid: 'uid-node-1'}) SET f.color = 'blue'");

        TestUtil.testCall(db, "CALL apoc.static.get('keyProp') YIELD value RETURN value", (row) -> assertEquals("color", row.get("value") ) );
        TestUtil.testCall(db, "CALL apoc.static.get('valueProp') YIELD value RETURN value", (row) -> assertEquals("blue", row.get("value") ) );
    }

    @Test
    public void testTriggerData_AssignedNodeProperties_ByUid_before() {
        testTriggerData_AssignedNodeProperties_ByUid("before");
    }

    @Test
    public void testTriggerData_AssignedNodeProperties_ByUid_after() {
        testTriggerData_AssignedNodeProperties_ByUid("after");
    }

    private void testTriggerData_RemovedNodeProperties_ByLabel( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $txData AS txData WITH txData, txData." + REMOVED_NODE_PROPERTIES + ".byLabel[\\'Foo\\'][\\'`Foo`:`uid`:`uid-node-1`\\'][0] AS props" +
                " WITH props.key AS keyProp, props.oldValue AS oldValueProp" +
                " CALL apoc.static.set(\\'keyProp\\', keyProp) YIELD value" +
                " WITH oldValueProp, value AS v1" +
                " CALL apoc.static.set(\\'oldValueProp\\', oldValueProp) YIELD value" +
                " RETURN 1',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");

        db.executeTransactionally("CREATE (f:Foo {name:'Michael', uid: 'uid-node-1', color: 'blue'})");
        db.executeTransactionally("MATCH (f:Foo {name:'Michael', uid: 'uid-node-1'}) REMOVE f.color");

        TestUtil.testCall(db, "CALL apoc.static.get('keyProp') YIELD value RETURN value", (row) -> assertEquals("color", row.get("value") ) );
        TestUtil.testCall(db, "CALL apoc.static.get('oldValueProp') YIELD value RETURN value", (row) -> assertEquals("blue", row.get("value") ) );
    }

    @Test
    public void testTriggerData_RemovedNodeProperties_ByLabel_before() {
        testTriggerData_RemovedNodeProperties_ByLabel("before");
    }

    @Test
    public void testTriggerData_RemovedNodeProperties_ByLabel_after() {
        testTriggerData_RemovedNodeProperties_ByLabel("after");
    }

    private void testTriggerData_RemovedNodeProperties_ByKey( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $txData AS txData WITH txData, txData." + REMOVED_NODE_PROPERTIES + ".byKey.color[0] AS uidProp" +
                " CALL apoc.static.set(\\'uidProp\\', uidProp) YIELD value" +
                " RETURN 1',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");

        db.executeTransactionally("CREATE (f:Foo {name:'Michael', uid: 'uid-node-1', color: 'blue'})");
        db.executeTransactionally("MATCH (f:Foo {name:'Michael', uid: 'uid-node-1'}) REMOVE f.color");

        TestUtil.testCall(db, "CALL apoc.static.get('uidProp') YIELD value RETURN value", (row) -> assertEquals("`Foo`:`uid`:`uid-node-1`", row.get("value") ) );
    }

    @Test
    public void testTriggerData_RemovedNodeProperties_ByKey_before() {
        testTriggerData_RemovedNodeProperties_ByKey("before");
    }

    @Test
    public void testTriggerData_RemovedNodeProperties_ByKey_after() {
        testTriggerData_RemovedNodeProperties_ByKey("after");
    }

    private void testTriggerData_RemovedNodeProperties_ByUid( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $txData AS txData WITH txData, txData." + REMOVED_NODE_PROPERTIES + ".byUid[\\'`Foo`:`uid`:`uid-node-1`\\'][0] AS props" +
                " WITH props.key AS keyProp, props.oldValue AS oldValueProp" +
                " CALL apoc.static.set(\\'keyProp\\', keyProp) YIELD value" +
                " WITH oldValueProp, value AS v1" +
                " CALL apoc.static.set(\\'oldValueProp\\', oldValueProp) YIELD value" +
                " RETURN 1',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");

        db.executeTransactionally("CREATE (f:Foo {name:'Michael', uid: 'uid-node-1', color: 'blue'})");
        db.executeTransactionally("MATCH (f:Foo {name:'Michael', uid: 'uid-node-1'}) REMOVE f.color");

        TestUtil.testCall(db, "CALL apoc.static.get('keyProp') YIELD value RETURN value", (row) -> assertEquals("color", row.get("value") ) );
        TestUtil.testCall(db, "CALL apoc.static.get('oldValueProp') YIELD value RETURN value", (row) -> assertEquals("blue", row.get("value") ) );
    }

    @Test
    public void testTriggerData_RemovedNodeProperties_ByUid_before() {
        testTriggerData_RemovedNodeProperties_ByUid("before");
    }

    @Test
    public void testTriggerData_RemovedNodeProperties_ByUid_after() {
        testTriggerData_RemovedNodeProperties_ByUid("after");
    }

    private void testTriggerData_AssignedRelationshipProperties_ByType( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $txData AS txData WITH txData, txData." + ASSIGNED_RELATIONSHIP_PROPERTIES + ".byType[\\'BAR\\'][\\'`BAR`:`uid`:`uid-rel-1`\\'][0] AS props" +
                " WITH props.key AS keyProp, props.value AS valueProp" +
                " CALL apoc.static.set(\\'keyProp\\', keyProp) YIELD value" +
                " WITH valueProp, value AS v1" +
                " CALL apoc.static.set(\\'valueProp\\', valueProp) YIELD value" +
                " RETURN 1',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");

        db.executeTransactionally("CREATE (f:Foo {name:'Michael'})-[r:BAR {uid:'uid-rel-1'}]->(g:Foo {name:'John'})");
        db.executeTransactionally("MATCH (f:Foo)-[r:BAR]->(g:Foo) SET r.color = 'red'");

        TestUtil.testCall(db, "CALL apoc.static.get('keyProp') YIELD value RETURN value", (row) -> assertEquals("color", row.get("value") ) );
        TestUtil.testCall(db, "CALL apoc.static.get('valueProp') YIELD value RETURN value", (row) -> assertEquals("red", row.get("value") ) );
    }

    @Test
    public void testTriggerData_AssignedRelationshipProperties_ByType_before() {
        testTriggerData_AssignedRelationshipProperties_ByType("before");
    }

    @Test
    public void testTriggerData_AssignedRelationshipProperties_ByType_after() {
        testTriggerData_AssignedRelationshipProperties_ByType("after");
    }

    private void testTriggerData_AssignedRelationshipProperties_ByKey( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $txData AS txData WITH txData, txData." + ASSIGNED_RELATIONSHIP_PROPERTIES + ".byKey.color[0] AS uidProp" +
                " CALL apoc.static.set(\\'uidProp\\', uidProp) YIELD value" +
                " RETURN 1',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");

        db.executeTransactionally("CREATE (f:Foo {name:'Michael'})-[r:BAR {uid:'uid-rel-1'}]->(g:Foo {name:'John'})");
        db.executeTransactionally("MATCH (f:Foo)-[r:BAR]->(g:Foo) SET r.color = 'red'");

        TestUtil.testCall(db, "CALL apoc.static.get('uidProp') YIELD value RETURN value", (row) -> assertEquals("`BAR`:`uid`:`uid-rel-1`", row.get("value") ) );
    }

    @Test
    public void testTriggerData_AssignedRelationshipProperties_ByKey_before() {
        testTriggerData_AssignedRelationshipProperties_ByKey("before");
    }

    @Test
    public void testTriggerData_AssignedRelationshipProperties_ByKey_after() {
        testTriggerData_AssignedRelationshipProperties_ByKey("after");
    }

    private void testTriggerData_AssignedRelationshipProperties_ByUid( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $txData AS txData WITH txData, txData." + ASSIGNED_RELATIONSHIP_PROPERTIES + ".byUid[\\'`BAR`:`uid`:`uid-rel-1`\\'][\\'propertyChanges\\'][0] AS props" +
                " WITH props.key AS keyProp, props.value AS valueProp" +
                " CALL apoc.static.set(\\'keyProp\\', keyProp) YIELD value" +
                " WITH valueProp, value AS v1" +
                " CALL apoc.static.set(\\'valueProp\\', valueProp) YIELD value" +
                " RETURN 1',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");

        db.executeTransactionally("CREATE (f:Foo {name:'Michael'})-[r:BAR {uid:'uid-rel-1'}]->(g:Foo {name:'John'})");
        db.executeTransactionally("MATCH (f:Foo)-[r:BAR]->(g:Foo) SET r.color = 'red'");

        TestUtil.testCall(db, "CALL apoc.static.get('keyProp') YIELD value RETURN value", (row) -> assertEquals("color", row.get("value") ) );
        TestUtil.testCall(db, "CALL apoc.static.get('valueProp') YIELD value RETURN value", (row) -> assertEquals("red", row.get("value") ) );
    }

    @Test
    public void testTriggerData_AssignedRelationshipProperties_ByUid_before() {
        testTriggerData_AssignedRelationshipProperties_ByUid("before");
    }

    @Test
    public void testTriggerData_AssignedRelationshipProperties_ByUid_after() {
        testTriggerData_AssignedRelationshipProperties_ByUid("after");
    }

    private void testTriggerData_RemovedRelationshipProperties_ByType( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $txData AS txData WITH txData, txData." + REMOVED_RELATIONSHIP_PROPERTIES + ".byType[\\'BAR\\'][\\'`BAR`:`uid`:`uid-rel-1`\\'][0] AS props" +
                " WITH props.key AS keyProp, props.oldValue AS oldValueProp" +
                " CALL apoc.static.set(\\'keyProp\\', keyProp) YIELD value" +
                " WITH oldValueProp, value AS v1" +
                " CALL apoc.static.set(\\'oldValueProp\\', oldValueProp) YIELD value" +
                " RETURN 1',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");

        db.executeTransactionally("CREATE (f:Foo {name:'Michael'})-[r:BAR {uid:'uid-rel-1', color: 'red'}]->(g:Foo {name:'John'})");
        db.executeTransactionally("MATCH (f:Foo)-[r:BAR]->(g:Foo) REMOVE r.color");

        TestUtil.testCall(db, "CALL apoc.static.get('keyProp') YIELD value RETURN value", (row) -> assertEquals("color", row.get("value") ) );
        TestUtil.testCall(db, "CALL apoc.static.get('oldValueProp') YIELD value RETURN value", (row) -> assertEquals("red", row.get("value") ) );
    }

    @Test
    public void testTriggerData_RemovedRelationshipProperties_ByType_before() {
        testTriggerData_RemovedRelationshipProperties_ByType("before");
    }

    @Test
    public void testTriggerData_RemovedRelationshipProperties_ByType_after() {
        testTriggerData_RemovedRelationshipProperties_ByType("after");
    }

    private void testTriggerData_RemovedRelationshipProperties_ByKey( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $txData AS txData WITH txData, txData." + REMOVED_RELATIONSHIP_PROPERTIES + ".byKey.color[0] AS uidProp" +
                " CALL apoc.static.set(\\'uidProp\\', uidProp) YIELD value" +
                " RETURN 1',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");

        db.executeTransactionally("CREATE (f:Foo {name:'Michael'})-[r:BAR {uid:'uid-rel-1', color: 'red'}]->(g:Foo {name:'John'})");
        db.executeTransactionally("MATCH (f:Foo)-[r:BAR]->(g:Foo) REMOVE r.color");

        TestUtil.testCall(db, "CALL apoc.static.get('uidProp') YIELD value RETURN value", (row) -> assertEquals("`BAR`:`uid`:`uid-rel-1`", row.get("value") ) );
    }

    @Test
    public void testTriggerData_RemovedRelationshipProperties_ByKey_before() {
        testTriggerData_RemovedRelationshipProperties_ByKey("before");
    }

    @Test
    public void testTriggerData_RemovedRelationshipProperties_ByKey_after() {
        testTriggerData_RemovedRelationshipProperties_ByKey("after");
    }

    private void testTriggerData_RemovedRelationshipProperties_ByUid( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $txData AS txData WITH txData, txData." + REMOVED_RELATIONSHIP_PROPERTIES + ".byUid[\\'`BAR`:`uid`:`uid-rel-1`\\'][\\'propertyChanges\\'][0] AS props" +
                " WITH props.key AS keyProp, props.oldValue AS oldValueProp" +
                " CALL apoc.static.set(\\'keyProp\\', keyProp) YIELD value" +
                " WITH oldValueProp, value AS v1" +
                " CALL apoc.static.set(\\'oldValueProp\\', oldValueProp) YIELD value" +
                " RETURN 1',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");

        db.executeTransactionally("CREATE (f:Foo {name:'Michael'})-[r:BAR {uid:'uid-rel-1', color: 'red'}]->(g:Foo {name:'John'})");
        db.executeTransactionally("MATCH (f:Foo)-[r:BAR]->(g:Foo) REMOVE r.color");

        TestUtil.testCall(db, "CALL apoc.static.get('keyProp') YIELD value RETURN value", (row) -> assertEquals("color", row.get("value") ) );
        TestUtil.testCall(db, "CALL apoc.static.get('oldValueProp') YIELD value RETURN value", (row) -> assertEquals("red", row.get("value") ) );
    }

    @Test
    public void testTriggerData_RemovedRelationshipProperties_ByUid_before() {
        testTriggerData_RemovedRelationshipProperties_ByUid("before");
    }

    @Test
    public void testTriggerData_RemovedRelationshipProperties_ByUid_after() {
        testTriggerData_RemovedRelationshipProperties_ByUid("after");
    }

    private void testTriggerData_createAndDeleteSameTransaction( String phase ) {
        db.executeTransactionally("CALL apoc.trigger.add('test','WITH $txData AS txData RETURN 1',{phase: '" + phase + "'}, { params: {uidKeys: ['uid']}})");

        db.executeTransactionally("CREATE (f:Foo {name:'Michael'})-[r:BAR {uid:'uid-rel-1'}]->(g:Foo {name:'John'})");
        db.executeTransactionally("MATCH (f:Foo {name:'Michael'}) DETACH DELETE f");
    }

    @Test
    public void testTriggerData_createAndDeleteSameTransaction_before() {
        testTriggerData_createAndDeleteSameTransaction("before");
    }

    @Test
    public void testTriggerData_createAndDeleteSameTransaction_after() {
        testTriggerData_createAndDeleteSameTransaction("after");
    }
}
