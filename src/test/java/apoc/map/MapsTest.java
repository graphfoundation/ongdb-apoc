package apoc.map;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.*;

import static apoc.util.MapUtil.map;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.Iterators.asSet;

/**
 * @author mh
 * @since 04.05.16
 */
public class MapsTest {

    private GraphDatabaseService db;
    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Maps.class);
    }
    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testFromNodes() throws Exception {
        db.execute("UNWIND range(1,3) as id create (:Person {name:'name'+id})").close();
        TestUtil.testCall(db, "RETURN apoc.map.fromNodes('Person','name') as value", (r) -> {
            Map<String,Node> map = (Map<String, Node>) r.get("value");
            assertEquals(asSet("name1","name2","name3"),map.keySet());
            map.forEach((k,v) -> assertEquals(k, v.getProperty("name")));
        });
    }

    @Test
    public void testValues() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.values({b:42,a:'foo',c:false},['a','b','d']) as value", (r) -> {
            assertEquals(asList("foo",42L),r.get("value"));
        });
        TestUtil.testCall(db, "RETURN apoc.map.values({b:42,a:'foo',c:false},['a','b','d'],true) as value", (r) -> {
            assertEquals(asList("foo",42L,null),r.get("value"));
        });
        TestUtil.testCall(db, "RETURN apoc.map.values({b:42,a:'foo',c:false},null) as value", (r) -> {
            assertEquals(emptyList(),r.get("value"));
        });
        TestUtil.testCall(db, "RETURN apoc.map.values({b:42,a:'foo',c:false},[]) as value", (r) -> {
            assertEquals(emptyList(),r.get("value"));
        });
    }
    @Test
    public void testGroupBy() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.groupBy([{id:0,a:1},{id:1, b:false},{id:0,c:2}],'id') as value", (r) -> {
            assertEquals(map("0",map("id",0L,"c",2L),"1",map("id",1L,"b",false)),r.get("value"));
        });
    }
    @Test
    public void testGroupByMulti() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.groupByMulti([{id:0,a:1},{id:1, b:false},{id:0,c:2}],'id') as value", (r) -> {
            assertEquals(map("0",asList(map("id",0L,"a",1L),map("id",0L,"c",2L)),"1",asList(map("id",1L,"b",false))),r.get("value"));
        });
    }
    @Test
    public void testMerge() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.merge({a:1},{b:false}) AS value", (r) -> {
            assertEquals(map("a",1L,"b",false),r.get("value"));
        });
    }

    @Test
    public void testMergeList() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.mergeList([{a:1},{b:false}]) as value", (r) -> {
            assertEquals(map("a",1L,"b",false),r.get("value"));
        });
    }

    @Test
    public void testFromPairs() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.fromPairs([['a',1],['b',false]]) AS value", (r) -> {
            assertEquals(map("a",1L,"b",false),r.get("value"));
        });
    }
    @Test
    public void testFromValues() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.fromValues(['a',1,'b',false]) AS value", (r) -> {
            assertEquals(map("a",1L,"b",false),r.get("value"));
        });
    }

    @Test
    public void testFromLists() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.fromLists(['a','b'],[1,false]) AS value", (r) -> {
            assertEquals(map("a",1L,"b",false),r.get("value"));
        });
    }
    @Test
    public void testSetPairs() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.setPairs({}, [['a',1],['b',false]]) AS value", (r) -> {
            assertEquals(map("a",1L,"b",false),r.get("value"));
        });
    }
    @Test
    public void testSetValues() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.setValues({}, ['a',1,'b',false]) AS value", (r) -> {
            assertEquals(map("a",1L,"b",false),r.get("value"));
        });
    }

    @Test
    public void testSetLists() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.setLists({}, ['a','b'],[1,false]) AS value", (r) -> {
            assertEquals(map("a",1L,"b",false),r.get("value"));
        });
    }



    @Test
    public void testGet() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.get({a:1},'a') AS value", (r) -> {
            assertEquals(1L,r.get("value"));
        });
        TestUtil.testCall(db, "RETURN apoc.map.get({a:1},'c',42) AS value", (r) -> {
            assertEquals(42L,r.get("value"));
        });
        TestUtil.testCall(db, "RETURN apoc.map.get({a:1},'c',null,false) AS value", (r) -> {
            assertEquals(null,r.get("value"));
        });
        TestUtil.testFail(db, "RETURN apoc.map.get({a:1},'c') AS value", IllegalArgumentException.class);
    }

    @Test
    public void testSubMap() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.submap({a:1,b:1},['a']) AS value", (r) -> {
            assertEquals(map("a",1L),r.get("value"));
        });
        TestUtil.testCall(db, "RETURN apoc.map.submap({a:1,b:2},['a','b']) AS value", (r) -> {
            assertEquals(map("a",1L,"b",2L),r.get("value"));
        });
        TestUtil.testCall(db, "RETURN apoc.map.submap({a:1,b:1},['c'],[42]) AS value", (r) -> {
            assertEquals(map("c",42L),r.get("value"));
        });
        TestUtil.testCall(db, "RETURN apoc.map.submap({a:1,b:1},['c'],null,false) AS value", (r) -> {
            assertEquals(map("c",null),r.get("value"));
        });
        TestUtil.testFail(db, "RETURN apoc.map.submap({a:1,b:1},['c']) AS value", IllegalArgumentException.class);
        TestUtil.testFail(db, "RETURN apoc.map.submap({a:1,b:1},['a','c']) AS value", IllegalArgumentException.class);
    }

    @Test
    public void testMGet() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.mget({a:1,b:1},['a']) AS value", (r) -> {
            assertEquals(asList(1L),r.get("value"));
        });
        TestUtil.testCall(db, "RETURN apoc.map.mget({a:1,b:2},['a','b']) AS value", (r) -> {
            assertEquals(asList(1L,2L),r.get("value"));
        });
        TestUtil.testCall(db, "RETURN apoc.map.mget({a:1,b:1},['c'],[42]) AS value", (r) -> {
            assertEquals(asList(42L),r.get("value"));
        });
        TestUtil.testCall(db, "RETURN apoc.map.mget({a:1,b:1},['c'],null,false) AS value", (r) -> {
            assertEquals(singletonList(null),r.get("value"));
        });
        TestUtil.testFail(db, "RETURN apoc.map.mget({a:1,b:1},['c']) AS value", IllegalArgumentException.class);
        TestUtil.testFail(db, "RETURN apoc.map.mget({a:1,b:1},['a','c']) AS value", IllegalArgumentException.class);
    }

    @Test
    public void testSetKey() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.setKey({a:1},'a',2) AS value", (r) -> {
            assertEquals(map("a",2L),r.get("value"));
        });
    }
    @Test
    public void testSetEntry() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.setEntry({a:1},'a',2) AS value", (r) -> {
            assertEquals(map("a",2L),r.get("value"));
        });
    }

    @Test
    public void testRemoveKey() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.removeKey({a:1,b:2},'a') AS value", (r) -> {
            assertEquals(map("b",2L),r.get("value"));
        });
    }

    @Test
    public void testRemoveLastKey() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.removeKey({a:1},'a') AS value", (r) -> {
            assertEquals(map(),r.get("value"));
        });
    }

    @Test
    public void testRemoveKeyRecursively() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.removeKey({a:1,b:2,c:{a:3,b:4}},'a', {recursive:true}) AS value", (r) -> {
            assertEquals(map("b",2L, "c", map("b",4L)),r.get("value"));
        });
    }

    @Test
    public void testRemoveKeyRecursivelySimpleProperties() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.removeKey({a:1,b:2},'b', {recursive:true}) AS value", (r) -> {
            assertEquals(map("a",1L), r.get("value"));
        });
    }

    @Test
    public void testRemoveLastKeyRecursively() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.removeKey({a:1,b:2,c:{a:3}},'a', {recursive:true}) AS value", (r) -> {
            assertEquals(map("b",2L),r.get("value"));
        });
    }

    @Test
    public void testRemoveKeyRecursivelyIncludingCollectionOfMaps() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.removeKey({a:1,b:2,c:[{a:3,b:4}, {a:4,b:5}]},'a', {recursive:true}) AS value", (r) -> {
            assertEquals(map("b",2L, "c", asList(map("b",4L), map("b",5L))), r.get("value"));
        });
    }

    @Test
    public void testRemoveKeyRecursivelyIncludingCollectionOfStrings() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.removeKey({a:1,b:2,c:['a', 'b']},'a', {recursive:true}) AS value", (r) -> {
            assertEquals(map("b", 2L, "c", asList("a", "b")), r.get("value"));
        });
    }

    @Test
    public void testRemoveAllKeys() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.removeKeys({a:1,b:2},['a','b']) AS value", (r) -> {
            assertEquals(map(),r.get("value"));
        });
    }

    @Test
    public void testRemoveKeysRecursively() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.removeKeys({a:1,b:2,c:{a:3,b:4}},['a','b'], {recursive:true}) AS value", (r) -> {
            assertEquals(map(),r.get("value"));
        });
    }

    @Test
    public void testRemoveKeysRecursivelyIncludingCollectionOfMaps() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.removeKeys({a:1,b:2,c:[{a:3,b:4,d:1}, {a:4,b:5,d:3}]},['a','b'],{recursive:true}) AS value", (r) -> {
            assertEquals(map("c", asList(map("d",1L),map("d",3L))), r.get("value"));
        });
    }
    @Test
    public void testRemoveKeysRecursivelyRemovingCollectionCompletely() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.removeKeys({a:1,b:2,c:[{d:1}, {b:5,d:3}]},['d','b'],{recursive:true}) AS value", (r) -> {
            assertEquals(map("a", 1L), r.get("value"));
        });
    }

    @Test
    public void testRemoveKeysRecursivelyIncludingCollectionOfInts() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.removeKeys({a:1,b:2,c:[1,2,3]},['a','b'],{recursive:true}) AS value", (r) -> {
            assertEquals(map("c", asList(1L, 2L, 3L)), r.get("value"));
        });
    }

    @Test
    public void testClean() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.clean({a:1,b:'',c:null,x:1234,z:false},['x'],['',false]) AS value", (r) -> {
            assertEquals(map("a",1L),r.get("value"));
        });
    }

    @Test
    public void testUpdateTree() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.map.updateTree({id:1,c:{id:2},d:[{id:3}]},'id',[[1,{a:1}],[2,{a:2}],[3,{a:3}]]) AS value", (r) -> {
            assertEquals(map("id",1L,"a",1L,"c",map("id",2L,"a",2L),"d",asList(map("id",3L,"a",3L))),r.get("value"));
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFlatten() {
        Map<String, Object> nestedMap = map("somekey", "someValue", "somenumeric", 123);
        nestedMap = map("anotherkey", "anotherValue", "nested", nestedMap);
        Map<String, Object> map = map("string", "value", "int", 10, "nested", nestedMap);

        TestUtil.testCall(db, "RETURN apoc.map.flatten({map}) AS value", map("map", map), (r) -> {
            Map<String, Object> resultMap = (Map<String, Object>)r.get("value");
            assertEquals(map("string", "value",
                    "int", 10,
                    "nested.anotherkey", "anotherValue",
                    "nested.nested.somekey", "someValue",
                    "nested.nested.somenumeric", 123), resultMap);
        });
    }

    @Test
    public void testFlattenWithDelimiter() {
        Map<String, Object> nestedMap = map("somekey", "someValue", "somenumeric", 123);
        nestedMap = map("anotherkey", "anotherValue", "nested", nestedMap);
        Map<String, Object> map = map("string", "value", "int", 10, "nested", nestedMap);

        TestUtil.testCall(db, "RETURN apoc.map.flatten({map}, '-') AS value", map("map", map), (r) -> {
            Map<String, Object> resultMap = (Map<String, Object>)r.get("value");
            assertEquals(map("string", "value",
                    "int", 10,
                    "nested-anotherkey", "anotherValue",
                    "nested-nested-somekey", "someValue",
                    "nested-nested-somenumeric", 123), resultMap);
        });
    }

    @SuppressWarnings("unchecked")
    public void testUnflatten() {
        Map<String, Object> flatMap = map( "string", "value", "int", 10, "nested.anotherkey", "anotherValue", "nested.nested.somekey", "someValue", "nested.nested.somenumeric", 123 );
        TestUtil.testCall( db, "RETURN apoc.map.unflatten($map) AS value", map( "map", flatMap ), (r) -> {
            Map<String, Object> resultMap = (Map<String, Object>)r.get( "value" );
            assertEquals( map( "string", "value", "int", 10, "nested", map( "anotherkey", "anotherValue", "nested", map( "somekey", "someValue", "somenumeric", 123 ) ) ), resultMap );
        } );
    }

    @Test (expected = QueryExecutionException.class)
    public void testUnflattenConflictingKeys() {
        Map<String, Object> flatMap = map( "key", "value", "key.nested", "anotherValue" );
        TestUtil.testCall( db, "RETURN apoc.map.unflatten($map) AS value", map( "map", flatMap ), (r) -> {} );
    }

    @Test
    public void testSortedProperties() {
        TestUtil.testCall(db, "WITH {b:8, d:3, a:2, E: 12, C:9} as map RETURN apoc.map.sortedProperties(map, false) AS sortedProperties", (r) -> {
            List<List<String>> sortedProperties = (List<List<String>>)r.get("sortedProperties");
            assertEquals(5, sortedProperties.size());
            assertEquals(asList("C", 9l), sortedProperties.get(0));
            assertEquals(asList("E", 12l), sortedProperties.get(1));
            assertEquals(asList("a", 2l), sortedProperties.get(2));
            assertEquals(asList("b", 8l), sortedProperties.get(3));
            assertEquals(asList("d", 3l), sortedProperties.get(4));
        });
    }

    @Test
    public void testCaseInsensitiveSortedProperties() {
        TestUtil.testCall(db, "WITH {b:8, d:3, a:2, E: 12, C:9} as map RETURN apoc.map.sortedProperties(map) AS sortedProperties", (r) -> {
            List<List<String>> sortedProperties = (List<List<String>>)r.get("sortedProperties");
            assertEquals(5, sortedProperties.size());
            assertEquals(asList("a", 2l), sortedProperties.get(0));
            assertEquals(asList("b", 8l), sortedProperties.get(1));
            assertEquals(asList("C", 9l), sortedProperties.get(2));
            assertEquals(asList("d", 3l), sortedProperties.get(3));
            assertEquals(asList("E", 12l), sortedProperties.get(4));
        });
    }
}
