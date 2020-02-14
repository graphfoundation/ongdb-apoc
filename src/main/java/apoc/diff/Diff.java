package apoc.diff;

import apoc.Description;
import apoc.util.MapUtil;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.procedure.*;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.List;

/**
 * @author Benjamin Clauss
 * @author bradnussbaum
 * @since 15.06.2018
 */
public class Diff {

    public static final String DIGEST_ALGORITHM = "MD5";

    @Context
    public GraphDatabaseService db;

    @UserFunction()
    @Description("apoc.diff.nodes([leftNode],[rightNode]) returns a detailed diff of both nodes")
    public Map<String, Object> nodes(@Name("leftNode") Node leftNode, @Name("rightNode") Node rightNode, @Name(value = "propertyExcludes", defaultValue = "") List<String> excludedPropertyKeys) {
        Map<String, Object> allLeftProperties = leftNode.getAllProperties().entrySet().stream()
                .filter(e -> !excludedPropertyKeys.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, Object> allRightProperties = rightNode.getAllProperties().entrySet().stream()
                .filter(e -> !excludedPropertyKeys.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<String, Object> result = new HashMap<>();
        result.put("leftOnly", getPropertiesOnlyLeft(allLeftProperties, allRightProperties));
        result.put("rightOnly", getPropertiesOnlyLeft(allRightProperties, allLeftProperties));
        result.put("inCommon", getPropertiesInCommon(allLeftProperties, allRightProperties));
        result.put("different", getPropertiesDiffering(allLeftProperties, allRightProperties));

        return result;
    }

    @UserFunction()
    @Description("apoc.diff.nodesFull(leftNode,rightNode) returns a detailed diff including relationships of both nodes")
    public Map<String, Object> nodesFull(@Name("leftNode") Node leftNode, @Name("rightNode") Node rightNode,
            @Name(value = "propertyExcludes", defaultValue = "") List<String> excludedPropertyKeys, 
            @Name(value = "uuidKey", defaultValue = "") String uuidKey,
            @Name(value = "delimiter", defaultValue = ":") String delimiter,
            @Name(value = "delimiter", defaultValue = "0") long partitions) {

        Map<String,Object> fullDiff = new HashMap<>();
        fullDiff.put("properties", nodes(leftNode, rightNode, excludedPropertyKeys));


        Set<String> leftLabels = StreamSupport.stream(leftNode.getLabels().spliterator(),false).map(Label::name).collect(Collectors.toSet()); 
        Set<String> rightLabels = StreamSupport.stream(rightNode.getLabels().spliterator(),false).map(Label::name).collect(Collectors.toSet()); 

        Map<String, Object> labels = new HashMap<>();
        labels.put("leftOnly", getLabelsOnlyLeft(leftLabels,rightLabels));
        labels.put("rightOnly", getLabelsOnlyLeft(rightLabels,leftLabels));
        labels.put("inCommon", getLabelsInCommon(leftLabels,rightLabels));
        fullDiff.put("labels", labels);


        Map<String,Object> leftEdges = new HashMap<>();
        populateNodeEdges(leftEdges, leftNode, excludedPropertyKeys, uuidKey, delimiter, partitions);
        Map<String,Object> rightEdges = new HashMap<>();
        populateNodeEdges(rightEdges, rightNode, excludedPropertyKeys, uuidKey, delimiter, partitions);

        Map<String,Object> edgeDiff = new HashMap<>();
        edgeDiff.put("leftOnly", getPropertiesOnlyLeft(leftEdges, rightEdges));
        edgeDiff.put("rightOnly", getPropertiesOnlyLeft(rightEdges, leftEdges));

        Map<String,Object> inCommon = new HashMap<>();
        edgeDiff.put("inCommon", inCommon);

        Map<String,Object> inCommonEdges = getPropertiesInCommon(leftEdges, rightEdges);
        inCommonEdges.forEach( (k,v) -> {
            Map<String,Map<String,Object>> lhsEdges = (Map<String,Map<String,Object>>) v;
            Map<String,Map<String,Object>> rhsEdges = (Map<String,Map<String,Object>>) rightEdges.get(k);

            Map<String,Object> edgeNodeDiff = new HashMap<>();
            edgeNodeDiff.put("leftOnly", getOnlyLeft(lhsEdges, rhsEdges));
            edgeNodeDiff.put("rightOnly", getOnlyLeft(rhsEdges, lhsEdges));
            edgeNodeDiff.put("inCommon", getInCommon(lhsEdges, rhsEdges));
            inCommon.put(k,edgeNodeDiff);
        });
        fullDiff.put("edges", edgeDiff);
        return fullDiff;
    }

    private void populateNodeEdges(Map<String,Object> edges, Node node, List<String> excludedPropertyKeys,
        String uuidKey, String delimiter, long partitions) {
        node.getRelationships().forEach( edge -> {
            Node otherNode = edge.getOtherNode(node);
            String uuid = String.valueOf(otherNode.getProperty(uuidKey, otherNode.getId()));
            Map<String,Map<String,Object>> otherEdges = (Map<String,Map<String,Object>>) edges.getOrDefault(uuid, new HashMap<String,Map<String,Object>>());
            edges.put(transformUuid(uuid, delimiter, partitions), otherEdges);
            Map<String,Object> edgeData = new HashMap<>();
            edgeData.put("properties", edge.getAllProperties().entrySet().stream()
                .filter(e -> !excludedPropertyKeys.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            edgeData.put("type", edge.getType().name());
            edgeData.put("direction", node.equals(edge.getEndNode()) 
                ? node.equals(edge.getStartNode())
                    ? Direction.BOTH.name() 
                    : Direction.OUTGOING.name() 
                : Direction.INCOMING.name() );
            String signature = withMessageDigest(md -> fingerprint(md, edgeData, excludedPropertyKeys));
            otherEdges.put(signature,edgeData);
        });
    }

    private String transformUuid(String uuid, String delimiter, long partitions) {
        int i = 0;
        while (i < partitions && uuid.indexOf(delimiter) != -1) {
            uuid = uuid.substring(uuid.indexOf(delimiter)+1);
            i++;
        }
        return uuid;
    }

    public String fingerprint(Object thing, List<String> excludedPropertyKeys) {
        return withMessageDigest(md -> fingerprint(md, thing, excludedPropertyKeys));
    }

    private void fingerprint(MessageDigest md, Object thing, List<String> excludedPropertyKeys) {
        if (thing instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) thing;
            map.entrySet().stream()
                    .filter(e -> !excludedPropertyKeys.contains(e.getKey()))
                    .sorted(Map.Entry.comparingByKey())
                    .forEachOrdered(entry -> {
                md.update(entry.getKey().getBytes());
                md.update(fingerprint(entry.getValue(), excludedPropertyKeys).getBytes());
            });
        } else if (thing instanceof List) {
            List list = (List) thing;
            list.stream().forEach(o -> fingerprint(md, o, excludedPropertyKeys));
        } else {
            md.update(convertValueToString(thing).getBytes());
        }
    }

    private String withMessageDigest(Consumer<MessageDigest> consumer) {
        try {
            MessageDigest md = MessageDigest.getInstance(DIGEST_ALGORITHM);
            consumer.accept(md);
            return DatatypeConverter.printHexBinary(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private String convertValueToString(Object value) {
        if (value.getClass().isArray()) {
            return nativeArrayToString(value);
        } else {
            return value.toString();
        }
    }

    private String nativeArrayToString(Object value) {
        StringBuilder sb = new StringBuilder();
        if (value instanceof String[]) {
            for (String s : (String[]) value) {
                sb.append(s);
            }
        } else if (value instanceof double[]) {
            for (double d : (double[]) value) {
                sb.append(d);
            }
        } else if (value instanceof long[]) {
            for (double l : (long[]) value) {
                sb.append(l);
            }
        } else {
            throw new UnsupportedOperationException("cannot yet deal with " + value.getClass().getName());
        }
        return sb.toString();
    }

    private Map<String, Map<String,Object>> getOnlyLeft(Map<String, Map<String,Object>> left, Map<String, Map<String,Object>> right) {
        Map<String, Map<String,Object>> leftOnly = new HashMap<>();
        leftOnly.putAll(left);
        leftOnly.keySet().removeAll(right.keySet());
        return leftOnly;
    }

    private Map<String, Map<String,Object>> getInCommon(Map<String, Map<String,Object>> left, Map<String, Map<String,Object>> right) {
        Map<String, Map<String,Object>> inCommon = new HashMap<>(left);
        inCommon.entrySet().retainAll(right.entrySet());
        return inCommon;
    }

    private Map<String, Object> getPropertiesOnlyLeft(Map<String, Object> left, Map<String, Object> right) {
        Map<String, Object> leftOnly = new HashMap<>();
        leftOnly.putAll(left);
        leftOnly.keySet().removeAll(right.keySet());
        return leftOnly;
    }

    private Map<String, Object> getPropertiesInCommon(Map<String, Object> left, Map<String, Object> right) {
        Map<String, Object> inCommon = new HashMap<>(left);
        inCommon.entrySet().retainAll(right.entrySet());
        return inCommon;
    }

    private Map<String, Map<String, Object>> getPropertiesDiffering(Map<String, Object> left, Map<String, Object> right) {
        Map<String, Map<String, Object>> different = new HashMap<>();
        Map<String, Object> keyPairs = new HashMap<>();
        keyPairs.putAll(left);
        keyPairs.keySet().retainAll(right.keySet());

        for (Map.Entry<String, Object> entry : keyPairs.entrySet()) {
            if (!left.get(entry.getKey()).equals(right.get(entry.getKey()))) {
                Map<String, Object> pairs = new HashMap<>();
                pairs.put("left", left.get(entry.getKey()));
                pairs.put("right", right.get(entry.getKey()));
                different.put(entry.getKey(), pairs);
            }
        }
        return different;
    }

    private Set<String> getLabelsOnlyLeft(Set<String> left, Set<String> right) {
        Set<String> leftOnly = new HashSet<>(left);
        leftOnly.removeAll(right);
        return leftOnly;
    }

    private Set<String> getLabelsInCommon(Set<String> left, Set<String> right) {
        Set<String> inCommon = new HashSet<>(left);
        inCommon.retainAll(right);
        return inCommon;
    }
}
