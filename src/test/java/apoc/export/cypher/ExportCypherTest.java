package apoc.export.cypher;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import org.junit.*;
import org.junit.rules.TestName;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;

import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.*;
import static apoc.export.util.ExportFormat.*;
import static apoc.util.Util.map;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCypherTest {

    private static final Map<String, Object> exportConfig = map("useOptimizations", map("type", "none"),"separateFiles", true);
    private static GraphDatabaseService db;
    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Rule
    public TestName testName = new TestName();

    private static final String OPTIMIZED = "Optimized";
    private static final String ODD = "Odd";

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig(GraphDatabaseSettings.load_csv_file_url_root, directory.getAbsolutePath())
                .setConfig("apoc.export.file.enabled", "true")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, ExportCypher.class, Graphs.class);
        if (testName.getMethodName().endsWith(OPTIMIZED)) {
            db.execute("CREATE INDEX ON :Foo(name)").close();
            db.execute("CREATE INDEX ON :Bar(first_name, last_name)").close();
            db.execute("CREATE CONSTRAINT ON (b:Bar) ASSERT b.name IS UNIQUE").close();
            db.execute("CREATE (f:Foo {name:'foo', born:date('2018-10-31')})-[:KNOWS {since:2016}]->(b:Bar {name:'bar',age:42}),(c:Bar:Person {age:12}),(d:Bar {age:12})," +
                    " (t:Foo {name:'foo2', born:date('2017-09-29')})-[:KNOWS {since:2015}]->(e:Bar {name:'bar2',age:44}),({age:99})").close();
        } else if(testName.getMethodName().endsWith(ODD)) {
            db.execute("CREATE INDEX ON :Foo(name)").close();
            db.execute("CREATE INDEX ON :Bar(first_name, last_name)").close();
            db.execute("CREATE CONSTRAINT ON (b:Bar) ASSERT b.name IS UNIQUE").close();
            db.execute("CREATE (f:Foo {name:'foo', born:date('2018-10-31')})," +
                    "(t:Foo {name:'foo2', born:date('2017-09-29')})," +
                    "(g:Foo {name:'foo3', born:date('2016-03-12')})," +
                    "(b:Bar {name:'bar',age:42})," +
                    "(c:Bar {age:12})," +
                    "(d:Bar {age:4})," +
                    "(e:Bar {name:'bar2',age:44})," +
                    "(f)-[:KNOWS {since:2016}]->(b)").close();
        } else {
            db.execute("CREATE INDEX ON :Foo(name)").close();
            db.execute("CREATE INDEX ON :Bar(first_name, last_name)").close();
            db.execute("CREATE CONSTRAINT ON (b:Bar) ASSERT b.name IS UNIQUE").close();
            db.execute("CREATE (f:Foo {name:'foo', born:date('2018-10-31')})-[:KNOWS {since:2016}]->(b:Bar {name:'bar',age:42}),(c:Bar {age:12})").close();
        }
    }

    @After
    public void tearDown() {
        db.shutdown();
    }


    @Test
    public void testExportAllCypherResults() {
        TestUtil.testCall(db, "CALL apoc.export.cypher.all(null,{useOptimizations: { type: 'none'}})", (r) -> {
            assertResults(null, r, "database");
            assertEquals(EXPECTED_ONGDB_SHELL, r.get("cypherStatements"));
        });
    }

    @Test
    public void testExportAllCypherStreaming() {
        StringBuilder sb = new StringBuilder();
        TestUtil.testResult(db, "CALL apoc.export.cypher.all(null,{useOptimizations: { type: 'none'}, streamStatements:true,batchSize:3})", (res) -> {
            Map<String, Object> r = res.next();
            assertEquals(3L, r.get("batchSize"));
            assertEquals(1L, r.get("batches"));
            assertEquals(3L, r.get("nodes"));
            assertEquals(3L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(5L, r.get("properties"));
            assertNull("Should get file",r.get("file"));
            assertEquals("cypher", r.get("format"));
            assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
            sb.append(r.get("cypherStatements"));
            r = res.next();
            assertEquals(3L, r.get("batchSize"));
            assertEquals(2L, r.get("batches"));
            assertEquals(3L, r.get("nodes"));
            assertEquals(4L, r.get("rows"));
            assertEquals(1L, r.get("relationships"));
            assertEquals(6L, r.get("properties"));
            assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
            sb.append(r.get("cypherStatements"));
        });
        assertEquals(EXPECTED_ONGDB_SHELL.replace("LIMIT 20000", "LIMIT 3"), sb.toString());
    }

    // -- Whole file test -- //
    @Test
    public void testExportAllCypherDefault() throws Exception {
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({fileName},{useOptimizations: { type: 'none'}})",
                map("fileName", fileName),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_ONGDB_SHELL, readFile(fileName));
    }

    @Test
    public void testExportAllCypherForCypherShell() throws Exception {
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{config})",
                map("file", fileName, "config", map("useOptimizations", map("type", "none"), "format", "cypher-shell")),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_CYPHER_SHELL, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherForNeo4j() throws Exception {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell")), (r) -> {
                });
        assertEquals(EXPECTED_ONGDB_SHELL, readFile(fileName));
    }

    private static String readFile(String fileName) throws FileNotFoundException {
        return TestUtil.readFileToString(new File(directory, fileName));
    }

    @Test
    public void testExportGraphCypher() throws Exception {
        String fileName = "graph.cypher";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                "CALL apoc.export.cypher.graph(graph, {file},{useOptimizations: { type: 'none'}}) " +
                "YIELD nodes, relationships, properties, file, source,format, time " +
                "RETURN *", map("file", fileName), (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_ONGDB_SHELL, readFile(fileName));
    }

    // -- Separate files tests -- //
    @Test
    public void testExportAllCypherNodes() throws Exception {
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_NODES, readFile("all.nodes.cypher"));
    }

    @Test
    public void testExportAllCypherRelationships() throws Exception {
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_RELATIONSHIPS, readFile("all.relationships.cypher"));
    }

    @Test
    public void testExportAllCypherSchema() throws Exception {
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_SCHEMA, readFile("all.schema.cypher"));
    }

    @Test
    public void testExportAllCypherCleanUp() throws Exception {
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{exportConfig})", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_CLEAN_UP, readFile("all.cleanup.cypher"));
    }

    @Test
    public void testExportGraphCypherNodes() throws Exception {
        String fileName = "graph.cypher";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                "YIELD nodes, relationships, properties, file, source,format, time " +
                "RETURN *", map("file", fileName, "exportConfig", exportConfig), (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_NODES, readFile("graph.nodes.cypher"));
    }

    @Test
    public void testExportGraphCypherRelationships() throws Exception {
        String fileName = "graph.cypher";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_RELATIONSHIPS, readFile("graph.relationships.cypher"));
    }

    @Test
    public void testExportGraphCypherSchema() throws Exception {
        String fileName = "graph.cypher";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_SCHEMA, readFile("graph.schema.cypher"));
    }

    @Test
    public void testExportGraphCypherCleanUp() throws Exception {
        String fileName = "graph.cypher";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, {file},{exportConfig}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_CLEAN_UP, readFile("graph.cleanup.cypher"));
    }

    private void assertResults(String fileName, Map<String, Object> r, final String source) {
        assertEquals(3L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(6L, r.get("properties"));
        assertEquals(fileName, r.get("file"));
        assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }

    @Test
    public void testExportQueryCypherPlainFormat() throws Exception {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "plain")), (r) -> {
                });
        assertEquals(EXPECTED_PLAIN, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherFormatUpdateAll() throws Exception {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell", "cypherFormat", "updateAll")), (r) -> {
                });
        assertEquals(EXPECTED_ONGDB_MERGE, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherFormatAddStructure() throws Exception {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell", "cypherFormat", "addStructure")), (r) -> {
                });
        assertEquals(EXPECTED_NODES_MERGE_ON_CREATE_SET + EXPECTED_SCHEMA_EMPTY + EXPECTED_RELATIONSHIPS + EXPECTED_CLEAN_UP_EMPTY, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherFormatUpdateStructure() throws Exception {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell", "cypherFormat", "updateStructure")), (r) -> {
                });
        assertEquals(EXPECTED_NODES_EMPTY + EXPECTED_SCHEMA_EMPTY + EXPECTED_RELATIONSHIPS_MERGE_ON_CREATE_SET + EXPECTED_CLEAN_UP_EMPTY, readFile(fileName));
    }

    @Test
    public void testExportSchemaCypher() throws Exception {
        String fileName = "onlySchema.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema({file},{exportConfig})", map("file", fileName, "exportConfig", exportConfig), (r) -> {
        });
        assertEquals(EXPECTED_ONLY_SCHEMA_ONGDB_SHELL, readFile(fileName));
    }

    @Test
    public void testExportSchemaCypherShell() throws Exception {
        String fileName = "onlySchema.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema({file},{exportConfig})",
                map("file", fileName, "exportConfig", map("useOptimizations", map("type", "none"), "format", "cypher-shell")),
                (r) -> {});
        assertEquals(EXPECTED_ONLY_SCHEMA_CYPHER_SHELL, readFile(fileName));
    }

    @Test
    public void testExportCypherNodePoint() throws FileNotFoundException {
        db.execute("CREATE (f:Test {name:'foo'," +
                "place2d:point({ x: 2.3, y: 4.5 })," +
                "place3d1:point({ x: 2.3, y: 4.5 , z: 1.2})})" +
                "-[:FRIEND_OF {place2d:point({ longitude: 56.7, latitude: 12.78 })}]->" +
                "(:Bar {place3d:point({ longitude: 12.78, latitude: 56.7, height: 100 })})").close();
        String fileName = "temporalPoint.cypher";
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_POINT, readFile(fileName));
    }

    @Test
    public void testExportCypherNodeDate() throws FileNotFoundException {
        db.execute("CREATE (f:Test {name:'foo', " +
                "date:date('2018-10-30'), " +
                "datetime:datetime('2018-10-30T12:50:35.556+0100'), " +
                "localTime:localdatetime('20181030T19:32:24')})" +
                "-[:FRIEND_OF {date:date('2018-10-30')}]->" +
                "(:Bar {datetime:datetime('2018-10-30T12:50:35.556')})").close();
        String fileName = "temporalDate.cypher";
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_DATE, readFile(fileName));
    }

    @Test
    public void testExportCypherNodeTime() throws FileNotFoundException {
        db.execute("CREATE (f:Test {name:'foo', " +
                "local:localtime('12:50:35.556')," +
                "t:time('125035.556+0100')})" +
                "-[:FRIEND_OF {t:time('125035.556+0100')}]->" +
                "(:Bar {datetime:datetime('2018-10-30T12:50:35.556+0100')})").close();
        String fileName = "temporalTime.cypher";
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_TIME, readFile(fileName));
    }

    @Test
    public void testExportCypherNodeDuration() throws FileNotFoundException {
        db.execute("CREATE (f:Test {name:'foo', " +
                "duration:duration('P5M1.5D')})" +
                "-[:FRIEND_OF {duration:duration('P5M1.5D')}]->" +
                "(:Bar {duration:duration('P5M1.5D')})").close();
        String fileName = "temporalDuration.cypher";
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_DURATION, readFile(fileName));
    }

    @Test
    public void testExportWithAscendingLabels() throws FileNotFoundException {
        db.execute("CREATE (f:User:User1:User0:User12 {name:'Alan'})").close();
        String fileName = "ascendingLabels.cypher";
        String query = "MATCH (f:User) WHERE f.name='Alan' RETURN f";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query({query},{file},{config})",
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_LABELS_ASCENDEND, readFile(fileName));
    }

    @Test
    public void testExportAllCypherDefaultWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allDefaultOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})", map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_ONGDB_OPTIMIZED_BATCH_SIZE, readFile(fileName));
    }

    @Test
    public void testExportAllCypherDefaultOptimized() throws Exception {
        String fileName = "allDefaultOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file})", map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_ONGDB_OPTIMIZED, readFile(fileName));
    }

    @Test
    public void testExportAllCypherDefaultSeparatedFilesOptimized() throws Exception {
        String fileName = "allDefaultOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file}, {exportConfig})",
                map("file", fileName, "exportConfig", map("separateFiles", true)),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_NODES_OPTIMIZED, readFile("allDefaultOptimized.nodes.cypher"));
        assertEquals(EXPECTED_RELATIONSHIPS_OPTIMIZED, readFile("allDefaultOptimized.relationships.cypher"));
        assertEquals(EXPECTED_SCHEMA_OPTIMIZED, readFile("allDefaultOptimized.schema.cypher"));
        assertEquals(EXPECTED_CLEAN_UP, readFile("allDefaultOptimized.cleanup.cypher"));
    }

    @Test
    public void testExportAllCypherCypherShellWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allCypherShellOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{format:'cypher-shell', useOptimizations: {type: 'unwind_batch'}})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_CYPHER_SHELL_OPTIMIZED_BATCH_SIZE, readFile(fileName));
    }

    @Test
    public void testExportAllCypherCypherShellOptimized() throws Exception {
        String fileName = "allCypherShellOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{format:'cypher-shell'})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_CYPHER_SHELL_OPTIMIZED, readFile(fileName));
    }

    @Test
    public void testExportAllCypherPlainWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{format:'plain', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_PLAIN_OPTIMIZED_BATCH_SIZE, readFile(fileName));
    }

    @Test
    public void testExportAllCypherPlainAddStructureWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainAddStructureOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{format:'plain', cypherFormat: 'addStructure', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("file", fileName), (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_PLAIN_ADD_STRUCTURE_UNWIND, readFile(fileName));
    }

    @Test
    public void testExportAllCypherPlainUpdateStructureWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainUpdateStructureOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{format:'plain', cypherFormat: 'updateStructure', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("file", fileName), (r) -> {
                    assertEquals(0L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships"));
                    assertEquals(2L, r.get("properties"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("cypher", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
                });
        assertEquals(EXPECTED_PLAIN_UPDATE_STRUCTURE_UNWIND, readFile(fileName));
    }

    @Test
    public void testExportAllCypherPlainUpdateAllWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainUpdateAllOptimized.cypher";
        File output = new File(directory, fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{format:'plain', cypherFormat: 'updateAll', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("file", fileName), (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_UPDATE_ALL_UNWIND, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherShellWithUnwindBatchSizeWithBatchSizeOptimized() throws Exception {
        String fileName = "allPlainOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{format:'cypher-shell', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, batchSize: 2})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_UNWIND, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherShellWithUnwindBatchSizeWithBatchSizeOdd() throws Exception {
        String fileName = "allPlainOdd.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{format:'cypher-shell', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, batchSize: 2})",
                map("file", fileName), (r) -> assertResultsOdd(fileName, r));
        assertEquals(EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_ODD, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherShellWithUnwindBatchSizeWithBatchSizeParamsOdd() throws Exception {
        String fileName = "allPlainOdd.cypher";
        File output = new File(directory, fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all({file},{format:'cypher-shell', useOptimizations: { type: 'unwind_batch_params', unwindBatchSize: 2}, batchSize:2})",
                map("file", fileName),
                (r) -> assertResultsOdd(fileName, r));
        assertEquals(EXPECTED_QUERY_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD, readFile(fileName));
    }

    @Test
    @Ignore("non-deterministic index order")
    public void testExportAllCypherPlainOptimized() throws Exception {
        String fileName = "queryPlainOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query('MATCH (f:Foo)-[r:KNOWS]->(b:Bar) return f,r,b', {file},{format:'cypher-shell', useOptimizations: {type: 'unwind_batch'}})",
                map("file", fileName),
                (r) -> {
                    assertEquals(4L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships"));
                    assertEquals(10L, r.get("properties"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("statement: nodes(4), rels(2)", r.get("source"));
                    assertEquals("cypher", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
                });
        String actual = readFile(fileName);
        assertTrue("expected generated output",EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED.equals(actual) || EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED2.equals(actual));
    }

    private void assertResultsOptimized(String fileName, Map<String, Object> r) {
        assertEquals(7L, r.get("nodes"));
        assertEquals(2L, r.get("relationships"));
        assertEquals(13L, r.get("properties"));
        assertEquals(fileName, r.get("file"));
        assertEquals("database" + ": nodes(7), rels(2)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }

    private void assertResultsOdd(String fileName, Map<String, Object> r) {
        assertEquals(7L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(13L, r.get("properties"));
        assertEquals(fileName, r.get("file"));
        assertEquals("database" + ": nodes(7), rels(1)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }

    static class ExportCypherResults {

        static final String EXPECTED_NODES = String.format("BEGIN%n" +
                "CREATE (:Foo:`UNIQUE IMPORT LABEL` {born:date('2018-10-31'), name:\"foo\", `UNIQUE IMPORT ID`:0});%n" +
                "CREATE (:Bar {age:42, name:\"bar\"});%n" +
                "CREATE (:Bar:`UNIQUE IMPORT LABEL` {age:12, `UNIQUE IMPORT ID`:2});%n" +
                "COMMIT%n");

        private static final String EXPECTED_NODES_MERGE = String.format("BEGIN%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}) SET n.name=\"foo\", n.born=date('2018-10-31'), n:Foo;%n" +
                "MERGE (n:Bar{name:\"bar\"}) SET n.age=42;%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}) SET n.age=12, n:Bar;%n" +
                "COMMIT%n");

        static final String EXPECTED_NODES_MERGE_ON_CREATE_SET =
                EXPECTED_NODES_MERGE.replaceAll(" SET ", " ON CREATE SET ");

        static final String EXPECTED_NODES_EMPTY = String.format("BEGIN%n" +
                "COMMIT%n");

        static final String EXPECTED_SCHEMA = String.format("BEGIN%n" +
                "CREATE INDEX ON :Bar(first_name,last_name);%n" +
                "CREATE INDEX ON :Foo(name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_SCHEMA_EMPTY = String.format("BEGIN%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        private static final String EXPECTED_INDEXES_AWAIT = String.format("CALL db.awaitIndex(':Foo(name)');%n" +
                "CALL db.awaitIndex(':Bar(first_name,last_name)');%n" +
                "CALL db.awaitIndex(':Bar(name)');%n");

        private static final String EXPECTED_INDEXES_AWAIT_QUERY = String.format("CALL db.awaitIndex(':Foo(name)');%n" +
                "CALL db.awaitIndex(':Bar(name)');%n" +
                "CALL db.awaitIndex(':Bar(first_name,last_name)');%n");

        static final String EXPECTED_RELATIONSHIPS = String.format("BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:Bar{name:\"bar\"}) CREATE (n1)-[r:KNOWS {since:2016}]->(n2);%n" +
                "COMMIT%n");

        private static final String EXPECTED_RELATIONSHIPS_MERGE = String.format("BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:Bar{name:\"bar\"}) MERGE (n1)-[r:KNOWS]->(n2) SET r.since=2016;%n" +
                "COMMIT%n");

        static final String EXPECTED_RELATIONSHIPS_MERGE_ON_CREATE_SET =
                EXPECTED_RELATIONSHIPS_MERGE.replaceAll(" SET ", " ON CREATE SET ");

        static final String EXPECTED_CLEAN_UP = String.format("BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_CLEAN_UP_EMPTY = String.format("BEGIN%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "COMMIT%n");

        static final String EXPECTED_ONLY_SCHEMA_ONGDB_SHELL = String.format("BEGIN%n" +
                "CREATE INDEX ON :Bar(first_name,last_name);%n" +
                "CREATE INDEX ON :Foo(name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_CYPHER_POINT = String.format("BEGIN%n" +
                "CREATE (:Test:`UNIQUE IMPORT LABEL` {name:\"foo\", place2d:point({x: 2.3, y: 4.5, crs: 'cartesian'}), place3d1:point({x: 2.3, y: 4.5, z: 1.2, crs: 'cartesian-3d'}), `UNIQUE IMPORT ID`:20});%n" +
                "CREATE (:Bar:`UNIQUE IMPORT LABEL` {place3d:point({x: 12.78, y: 56.7, z: 100.0, crs: 'wgs-84-3d'}), `UNIQUE IMPORT ID`:21});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE INDEX ON :Bar(first_name,last_name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:20}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:21}) CREATE (n1)-[r:FRIEND_OF {place2d:point({x: 56.7, y: 12.78, crs: 'wgs-84'})}]->(n2);%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_CYPHER_DATE = String.format("BEGIN%n" +
                "CREATE (:Test:`UNIQUE IMPORT LABEL` {date:date('2018-10-30'), datetime:datetime('2018-10-30T12:50:35.556+01:00'), localTime:localdatetime('2018-10-30T19:32:24'), name:\"foo\", `UNIQUE IMPORT ID`:20});%n" +
                "CREATE (:Bar:`UNIQUE IMPORT LABEL` {datetime:datetime('2018-10-30T12:50:35.556Z'), `UNIQUE IMPORT ID`:21});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE INDEX ON :Bar(first_name,last_name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:20}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:21}) CREATE (n1)-[r:FRIEND_OF {date:date('2018-10-30')}]->(n2);%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_CYPHER_TIME = String.format("BEGIN%n" +
                "CREATE (:Test:`UNIQUE IMPORT LABEL` {local:localtime('12:50:35.556'), name:\"foo\", t:time('12:50:35.556+01:00'), `UNIQUE IMPORT ID`:20});%n" +
                "CREATE (:Bar:`UNIQUE IMPORT LABEL` {datetime:datetime('2018-10-30T12:50:35.556+01:00'), `UNIQUE IMPORT ID`:21});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE INDEX ON :Bar(first_name,last_name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:20}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:21}) CREATE (n1)-[r:FRIEND_OF {t:time('12:50:35.556+01:00')}]->(n2);%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_CYPHER_DURATION = String.format("BEGIN%n" +
                "CREATE (:Test:`UNIQUE IMPORT LABEL` {duration:duration('P5M1DT12H'), name:\"foo\", `UNIQUE IMPORT ID`:20});%n" +
                "CREATE (:Bar:`UNIQUE IMPORT LABEL` {duration:duration('P5M1DT12H'), `UNIQUE IMPORT ID`:21});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE INDEX ON :Bar(first_name,last_name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:20}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:21}) CREATE (n1)-[r:FRIEND_OF {duration:duration('P5M1DT12H')}]->(n2);%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_CYPHER_LABELS_ASCENDEND = String.format("BEGIN%n" +
                "CREATE (:User:User0:User1:User12:`UNIQUE IMPORT LABEL` {name:\"Alan\", `UNIQUE IMPORT ID`:20});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_SCHEMA_OPTIMIZED = String.format("BEGIN%n" +
                "CREATE INDEX ON :Bar(first_name,last_name);%n" +
                "CREATE INDEX ON :Foo(name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_NODES_OPTIMIZED_BATCH_SIZE = String.format("BEGIN%n" +
                "UNWIND [{_id:3, properties:{age:12}}] as row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                "UNWIND [{_id:2, properties:{age:12}}] as row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar:Person;%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] as row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] as row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "UNWIND [{_id:6, properties:{age:99}}] as row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_NODES_OPTIMIZED = String.format("BEGIN%n" +
                "UNWIND [{_id:3, properties:{age:12}}] as row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                "UNWIND [{_id:2, properties:{age:12}}] as row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar:Person;%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] as row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] as row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "UNWIND [{_id:6, properties:{age:99}}] as row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_QUERY_NODES_OPTIMIZED = String.format("BEGIN%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] as row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] as row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_QUERY_NODES_OPTIMIZED2 = String.format("BEGIN%n" +
                "UNWIND [{_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}, {_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}] as row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] as row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_RELATIONSHIPS_OPTIMIZED = String.format("BEGIN%n" +
                "UNWIND [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}, {start: {_id:4}, end: {name:\"bar2\"}, properties:{since:2015}}] as row%n" +
                "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n" +
                "MATCH (end:Bar{name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_RELATIONSHIPS_ODD = String.format("BEGIN%n" +
                "UNWIND [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}] as row%n" +
                "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n" +
                "MATCH (end:Bar{name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_RELATIONSHIPS_PARAMS_ODD = String.format(":param rows => [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}]%n" +
                "BEGIN%n" +
                "UNWIND $rows AS row%n" +
                "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n" +
                "MATCH (end:Bar{name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n" +
                "COMMIT%n");

        static final String DROP_UNIQUE_OPTIMIZED = String.format("BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        static final String DROP_UNIQUE_OPTIMIZED_BATCH = String.format("BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 2 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 2 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 2 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_NODES_OPTIMIZED_BATCH_SIZE_UNWIND = String.format("BEGIN%n" +
                "UNWIND [{_id:3, properties:{age:12}}] as row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{_id:2, properties:{age:12}}] as row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar:Person;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] as row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] as row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{_id:6, properties:{age:99}}] as row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_NODES_OPTIMIZED_BATCH_SIZE_ODD = String.format("BEGIN%n" +
                "UNWIND [{_id:4, properties:{age:12}}, {_id:5, properties:{age:4}}] as row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:1, properties:{born:date('2017-09-29'), name:\"foo2\"}}] as row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{_id:2, properties:{born:date('2016-03-12'), name:\"foo3\"}}] as row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] as row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_NODES_OPTIMIZED_PARAMS_BATCH_SIZE_ODD = String.format(":param rows => [{_id:4, properties:{age:12}}, {_id:5, properties:{age:4}}]%n" +
                ":begin%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                ":commit%n" +
                ":param rows => [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:1, properties:{born:date('2017-09-29'), name:\"foo2\"}}]%n" +
                ":begin%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                ":commit%n" +
                ":param rows => [{_id:2, properties:{born:date('2016-03-12'), name:\"foo3\"}}]%n" +
                ":begin%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                ":commit%n" +
                ":param rows => [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}]%n" +
                ":begin%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                ":commit%n");

        static final String EXPECTED_PLAIN_ADD_STRUCTURE_UNWIND = String.format("UNWIND [{_id:3, properties:{age:12}}] as row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties SET n:Bar;%n" +
                "UNWIND [{_id:2, properties:{age:12}}] as row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties SET n:Bar:Person;%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] as row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] as row%n" +
                "MERGE (n:Bar{name: row.name}) ON CREATE SET n += row.properties;%n" +
                "UNWIND [{_id:6, properties:{age:99}}] as row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties;%n" +
                "UNWIND [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}, {start: {_id:4}, end: {name:\"bar2\"}, properties:{since:2015}}] as row%n" +
                "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n" +
                "MATCH (end:Bar{name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end)  SET r += row.properties;%n");

        static final String EXPECTED_UPDATE_ALL_UNWIND = String.format("CREATE INDEX ON :Bar(first_name,last_name);%n" +
                "CREATE INDEX ON :Foo(name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "UNWIND [{_id:3, properties:{age:12}}] as row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                "UNWIND [{_id:2, properties:{age:12}}] as row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar:Person;%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] as row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] as row%n" +
                "MERGE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "UNWIND [{_id:6, properties:{age:99}}] as row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties;%n" +
                "UNWIND [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}, {start: {_id:4}, end: {name:\"bar2\"}, properties:{since:2015}}] as row%n" +
                "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n" +
                "MATCH (end:Bar{name: row.end.name})%n" +
                "MERGE (start)-[r:KNOWS]->(end) SET r += row.properties;%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n");

        static final String EXPECTED_PLAIN_UPDATE_STRUCTURE_UNWIND = String.format("UNWIND [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}, {start: {_id:4}, end: {name:\"bar2\"}, properties:{since:2015}}] as row%n" +
                "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n" +
                "MATCH (end:Bar{name: row.end.name})%n" +
                "MERGE (start)-[r:KNOWS]->(end) SET r += row.properties;%n");

        static final String EXPECTED_ONGDB_OPTIMIZED = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;

        static final String EXPECTED_ONGDB_OPTIMIZED_BATCH_SIZE = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED_BATCH_SIZE + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;

        static final String EXPECTED_ONGDB_SHELL_OPTIMIZED = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;

        static final String EXPECTED_ONGDB_SHELL_OPTIMIZED_BATCH_SIZE = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED_BATCH_SIZE + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;

        static final String EXPECTED_QUERY_NODES =  EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_QUERY_NODES_OPTIMIZED + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;
        static final String EXPECTED_QUERY_NODES2 =  EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_QUERY_NODES_OPTIMIZED2 + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;

        static final String EXPECTED_CYPHER_OPTIMIZED_BATCH_SIZE_UNWIND = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED_BATCH_SIZE_UNWIND + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED_BATCH;

        static final String EXPECTED_CYPHER_OPTIMIZED_BATCH_SIZE_ODD = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED_BATCH_SIZE_ODD + EXPECTED_RELATIONSHIPS_ODD + DROP_UNIQUE_OPTIMIZED_BATCH;

        static final String EXPECTED_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED_PARAMS_BATCH_SIZE_ODD + EXPECTED_RELATIONSHIPS_PARAMS_ODD + DROP_UNIQUE_OPTIMIZED_BATCH;


        static final String EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_UNWIND = EXPECTED_CYPHER_OPTIMIZED_BATCH_SIZE_UNWIND
                .replace(ONGDB_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(ONGDB_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(ONGDB_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(ONGDB_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());


        static final String EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_ODD = EXPECTED_CYPHER_OPTIMIZED_BATCH_SIZE_ODD
                .replace(ONGDB_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(ONGDB_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(ONGDB_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(ONGDB_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        static final String EXPECTED_QUERY_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD = EXPECTED_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD
                .replace(ONGDB_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(ONGDB_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(ONGDB_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(ONGDB_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        static final String EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED = EXPECTED_QUERY_NODES
                .replace(ONGDB_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(ONGDB_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(ONGDB_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT_QUERY)
                .replace(ONGDB_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        static final String EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED2 = EXPECTED_QUERY_NODES2
                .replace(ONGDB_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(ONGDB_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(ONGDB_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT_QUERY)
                .replace(ONGDB_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        static final String EXPECTED_CYPHER_SHELL_OPTIMIZED = EXPECTED_ONGDB_SHELL_OPTIMIZED
                .replace(ONGDB_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(ONGDB_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(ONGDB_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(ONGDB_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        static final String EXPECTED_CYPHER_SHELL_OPTIMIZED_BATCH_SIZE = EXPECTED_ONGDB_SHELL_OPTIMIZED_BATCH_SIZE
                .replace(ONGDB_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(ONGDB_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(ONGDB_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(ONGDB_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        static final String EXPECTED_PLAIN_OPTIMIZED_BATCH_SIZE = EXPECTED_ONGDB_SHELL_OPTIMIZED_BATCH_SIZE
                .replace(ONGDB_SHELL.begin(), PLAIN_FORMAT.begin())
                .replace(ONGDB_SHELL.commit(), PLAIN_FORMAT.commit())
                .replace(ONGDB_SHELL.schemaAwait(), PLAIN_FORMAT.schemaAwait());

        static final String EXPECTED_ONGDB_SHELL = EXPECTED_NODES + EXPECTED_SCHEMA + EXPECTED_RELATIONSHIPS + EXPECTED_CLEAN_UP;

        static final String EXPECTED_CYPHER_SHELL = EXPECTED_ONGDB_SHELL
                .replace(ONGDB_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(ONGDB_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(ONGDB_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(ONGDB_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        static final String EXPECTED_PLAIN = EXPECTED_ONGDB_SHELL
                .replace(ONGDB_SHELL.begin(), PLAIN_FORMAT.begin()).replace(ONGDB_SHELL.commit(), PLAIN_FORMAT.commit())
                .replace(ONGDB_SHELL.schemaAwait(), PLAIN_FORMAT.schemaAwait());

        static final String EXPECTED_ONGDB_MERGE = EXPECTED_NODES_MERGE + EXPECTED_SCHEMA + EXPECTED_RELATIONSHIPS_MERGE + EXPECTED_CLEAN_UP;

        static final String EXPECTED_ONLY_SCHEMA_CYPHER_SHELL = EXPECTED_ONLY_SCHEMA_ONGDB_SHELL.replace(ONGDB_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(ONGDB_SHELL.commit(), CYPHER_SHELL.commit()).replace(ONGDB_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait()) + EXPECTED_INDEXES_AWAIT;


        static final String EXPECTED_NODES_COMPOUND_CONSTRAINT = String.format("BEGIN%n" +
                "CREATE (:`Person` {`name`:\"John\", `surname`:\"Snow\"});%n" +
                "CREATE (:`Person` {`name`:\"Matt\", `surname`:\"Jackson\"});%n" +
                "CREATE (:`Person` {`name`:\"Jenny\", `surname`:\"White\"});%n" +
                "CREATE (:`Person` {`name`:\"Susan\", `surname`:\"Brown\"});%n" +
                "CREATE (:`Person` {`name`:\"Tom\", `surname`:\"Taylor\"});%n" +
                "COMMIT%n");

        static final String EXPECTED_SCHEMA_COMPOUND_CONSTRAINT = String.format("BEGIN%n" +
                "CREATE CONSTRAINT ON (node:`Person`) ASSERT (node.`name`, node.`surname`) IS NODE KEY;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_RELATIONSHIP_COMPOUND_CONSTRAINT = String.format(("BEGIN%n" +
                "MATCH (n1:`Person`{`surname`:\"Snow\", `name`:\"John\"}), (n2:`Person`{`surname`:\"Jackson\", `name`:\"Matt\"}) CREATE (n1)-[r:`KNOWS`]->(n2);%n" +
                "COMMIT%n"));

        static final String EXPECTED_INDEX_AWAIT_COMPOUND_CONSTRAINT =  String.format("CALL db.awaitIndex(':`Person`(`name`,`surname`)');%n");

        static final String EXPECTED_ONGDB_SHELL_WITH_COMPOUND_CONSTRAINT = String.format("BEGIN%n" +
                "CREATE CONSTRAINT ON (node:Person) ASSERT (node.name, node.surname) IS NODE KEY;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "UNWIND [{surname:\"Snow\", name:\"John\", properties:{}}, {surname:\"Jackson\", name:\"Matt\", properties:{}}, {surname:\"White\", name:\"Jenny\", properties:{}}, {surname:\"Brown\", name:\"Susan\", properties:{}}, {surname:\"Taylor\", name:\"Tom\", properties:{}}] as row%n" +
                "CREATE (n:Person{surname: row.surname, name: row.name}) SET n += row.properties;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{start: {name:\"John\", surname:\"Snow\"}, end: {name:\"Matt\", surname:\"Jackson\"}, properties:{}}] as row%n" +
                "MATCH (start:Person{surname: row.start.surname, name: row.start.name})%n" +
                "MATCH (end:Person{surname: row.end.surname, name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_CYPHER_SHELL_WITH_COMPOUND_CONSTRAINT = String.format(":begin%n" +
                "CREATE CONSTRAINT ON (node:Person) ASSERT (node.name, node.surname) IS NODE KEY;%n" +
                ":commit%n" +
                "CALL db.awaitIndex(':Person(name,surname)');%n" +
                ":begin%n" +
                "UNWIND [{surname:\"Snow\", name:\"John\", properties:{}}, {surname:\"Jackson\", name:\"Matt\", properties:{}}, {surname:\"White\", name:\"Jenny\", properties:{}}, {surname:\"Brown\", name:\"Susan\", properties:{}}, {surname:\"Taylor\", name:\"Tom\", properties:{}}] as row%n" +
                "CREATE (n:Person{surname: row.surname, name: row.name}) SET n += row.properties;%n" +
                ":commit%n" +
                ":begin%n" +
                "UNWIND [{start: {name:\"John\", surname:\"Snow\"}, end: {name:\"Matt\", surname:\"Jackson\"}, properties:{}}] as row%n" +
                "MATCH (start:Person{surname: row.start.surname, name: row.start.name})%n" +
                "MATCH (end:Person{surname: row.end.surname, name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n" +
                ":commit%n");

        static final String EXPECTED_PLAIN_FORMAT_WITH_COMPOUND_CONSTRAINT = String.format("CREATE CONSTRAINT ON (node:Person) ASSERT (node.name, node.surname) IS NODE KEY;%n" +
                "UNWIND [{surname:\"Snow\", name:\"John\", properties:{}}, {surname:\"Jackson\", name:\"Matt\", properties:{}}, {surname:\"White\", name:\"Jenny\", properties:{}}, {surname:\"Brown\", name:\"Susan\", properties:{}}, {surname:\"Taylor\", name:\"Tom\", properties:{}}] as row%n" +
                "CREATE (n:Person{surname: row.surname, name: row.name}) SET n += row.properties;%n" +
                "UNWIND [{start: {name:\"John\", surname:\"Snow\"}, end: {name:\"Matt\", surname:\"Jackson\"}, properties:{}}] as row%n" +
                "MATCH (start:Person{surname: row.start.surname, name: row.start.name})%n" +
                "MATCH (end:Person{surname: row.end.surname, name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n");
    }

}