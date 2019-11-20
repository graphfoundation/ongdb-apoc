package apoc.trigger;

import apoc.util.JsonUtil;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TransactionDataMap
{

    public static final String ARRAY_PREFIX = "Array";

    public static final String BY_LABEL = "byLabel";
    public static final String BY_TYPE = "byType";
    public static final String BY_UID = "byUid";
    public static final String BY_KEY = "byKey";

    public static final String TRANSACTION_ID = "transactionId";
    public static final String COMMIT_TIME = "commitTime";
    public static final String CREATED_NODES = "createdNodes";
    public static final String CREATED_RELATIONSHIPS = "createdRelationships";
    public static final String DELETED_NODES = "deletedNodes";
    public static final String DELETED_RELATIONSHIPS = "deletedRelationships";
    public static final String REMOVED_LABELS = "removedLabels";
    public static final String REMOVED_NODE_PROPERTIES = "removedNodeProperties";
    public static final String REMOVED_RELATIONSHIP_PROPERTIES = "removedRelationshipProperties";
    public static final String ASSIGNED_LABELS = "assignedLabels";
    public static final String ASSIGNED_NODE_PROPERTIES = "assignedNodeProperties";
    public static final String ASSIGNED_RELATIONSHIP_PROPERTIES = "assignedRelationshipProperties";


    public static Map<String,Object> toMap( TransactionDataMapObject t )
    {
        return JsonUtil.OBJECT_MAPPER.convertValue( t, Map.class );
    }

    public enum ActionType
    {
        ADDED,
        REMOVED

    }

    public static Map<String,Map<String,Object>> updatedNodeMap( TxDataWrapper txDataWrapper, Iterable<Node> updatedNodes, ActionType actionType )
    {
        Map<String,Map<String,Object>> nodeChanges = new HashMap<>();

        Iterator<Node> updatedNodesIterator = updatedNodes.iterator();
        while ( updatedNodesIterator.hasNext() )
        {
            Node node = updatedNodesIterator.next();
            String uniqueIdentifier = txDataWrapper.getNodeUid( node.getId() );

            nodeChanges.put( uniqueIdentifier, toMap( new NodeChange( actionType ) ) );
        }

        return nodeChanges.isEmpty() ? Collections.emptyMap() : nodeChanges;
    }

    public static Map<String,Map<String,Object>> createdNodeMap( TxDataWrapper txDataWrapper )
    {
        return updatedNodeMap( txDataWrapper, txDataWrapper.getTransactionData().createdNodes(), ActionType.ADDED );
    }

    public static Map<String,Map<String,Object>> deletedNodeMap( TxDataWrapper txDataWrapper )
    {
        return updatedNodeMap( txDataWrapper, txDataWrapper.getTransactionData().deletedNodes(), ActionType.REMOVED );
    }

    public static Map<String,Map<String,Object>> updatedRelationshipsMap( TxDataWrapper txDataWrapper, Iterable<Relationship> updatedRelationships, ActionType actionType )
    {
        Map<String,Map<String,Object>> relationshipChanges = new HashMap<>();

        Iterator<Relationship> updatedRelationshipsIterator = updatedRelationships.iterator();
        while ( updatedRelationshipsIterator.hasNext() )
        {
            Relationship relationship = updatedRelationshipsIterator.next();

            Node startNode = relationship.getStartNode();
            Node endNode = relationship.getEndNode();

            String relationshipUid = txDataWrapper.getRelationshipUid( relationship.getId() );
            String startNodeUid = txDataWrapper.getNodeUid( startNode.getId() );
            String endNodeUid = txDataWrapper.getNodeUid( endNode.getId() );

            relationshipChanges.put( relationshipUid,
                    toMap( new RelationshipChange( startNodeUid, endNodeUid, relationship.getType().name(), actionType ) ) );
        }

        return relationshipChanges.isEmpty() ? Collections.emptyMap() : relationshipChanges;
    }

    public static Map<String,Map<String,Object>> createdRelationshipsMap( TxDataWrapper txDataWrapper )
    {
        return updatedRelationshipsMap( txDataWrapper, txDataWrapper.getTransactionData().createdRelationships(), ActionType.ADDED );
    }

    public static Map<String,Map<String,Object>> deletedRelationshipsMap( TxDataWrapper txDataWrapper )
    {
        return updatedRelationshipsMap( txDataWrapper, txDataWrapper.getTransactionData().deletedRelationships(), ActionType.REMOVED );
    }

    public static Map<String,List<Map<String,Object>>> updatedLabelMapByLabel( TxDataWrapper txDataWrapper, Iterable<LabelEntry> assignedLabels,
            ActionType actionType )
    {
        Map<String,List<Map<String,Object>>> labelChanges = new HashMap<>();

        Iterator<LabelEntry> assignedLabelsIterator = assignedLabels.iterator();
        while ( assignedLabelsIterator.hasNext() )
        {
            LabelEntry labelEntry = assignedLabelsIterator.next();
            Node updatedNode = labelEntry.node();

            String label = labelEntry.label().name();
            String nodeUid = txDataWrapper.getNodeUid( updatedNode.getId() );

            if ( !labelChanges.containsKey( label ) )
            {
                labelChanges.put( label, new ArrayList<>() );
            }
            labelChanges.get( label ).add( toMap( new LabelChange( nodeUid, actionType ) ) );
        }

        return labelChanges.isEmpty() ? Collections.emptyMap() : labelChanges;
    }

    public static Map<String,List<Map<String,Object>>> assignedLabelMapByLabel( TxDataWrapper txDataWrapper )
    {
        return updatedLabelMapByLabel( txDataWrapper, txDataWrapper.getTransactionData().assignedLabels(), ActionType.ADDED );
    }

    public static Map<String,List<Map<String,Object>>> removedLabelMapByLabel( TxDataWrapper txDataWrapper )
    {
        return updatedLabelMapByLabel( txDataWrapper, txDataWrapper.getTransactionData().removedLabels(), ActionType.REMOVED );
    }

    public static Map<String,List<String>> updatedLabelMapByUid( TxDataWrapper txDataWrapper, Iterable<LabelEntry> assignedLabels,
            ActionType actionType )
    {
        Map<String,List<String>> uidTolabelsMap = new HashMap<>();

        Iterator<LabelEntry> assignedLabelsIterator = assignedLabels.iterator();
        while ( assignedLabelsIterator.hasNext() )
        {
            LabelEntry labelEntry = assignedLabelsIterator.next();
            Node updatedNode = labelEntry.node();

            String label = labelEntry.label().name();
            String nodeUid = txDataWrapper.getNodeUid( updatedNode.getId() );

            if ( !uidTolabelsMap.containsKey( nodeUid ) )
            {
                uidTolabelsMap.put( nodeUid, new ArrayList<>() );
            }
            uidTolabelsMap.get( nodeUid ).add( label );
        }

        return uidTolabelsMap.isEmpty() ? Collections.emptyMap() : uidTolabelsMap;
    }

    public static Map<String,List<String>> assignedLabelMapByUid( TxDataWrapper txDataWrapper )
    {
        return updatedLabelMapByUid( txDataWrapper, txDataWrapper.getTransactionData().assignedLabels(), ActionType.ADDED );
    }

    public static Map<String,List<String>> removedLabelMapByUid( TxDataWrapper txDataWrapper )
    {
        return updatedLabelMapByUid( txDataWrapper, txDataWrapper.getTransactionData().removedLabels(), ActionType.REMOVED );
    }

    /**
     * Returns the updated node properties by key (property name) and value (list of node uids).
     */
    public static Map<String,List<String>> updatedNodePropertyMapByKey( TxDataWrapper txDataWrapper, Iterable<PropertyEntry<Node>> entityIterable,
            ActionType actionType )
    {
        Map<String,List<String>> propertyChanges = new HashMap<>();

        Iterator<PropertyEntry<Node>> entityIterator = entityIterable.iterator();
        while ( entityIterator.hasNext() )
        {
            PropertyEntry<Node> entityPropertyEntry = entityIterator.next();

            String propertyKey = entityPropertyEntry.key();
            Node updatedEntity = entityPropertyEntry.entity();

            String entityUid = txDataWrapper.getNodeUid( updatedEntity.getId() );

            if (!propertyChanges.containsKey( propertyKey ))
            {
                propertyChanges.put( propertyKey, new ArrayList<>(  ) );
            }

            propertyChanges.get( propertyKey ).add( entityUid );
        }

        return propertyChanges.isEmpty() ? Collections.emptyMap() : propertyChanges;
    }

    public static Map<String,List<String>> assignedNodePropertyMapByKey( TxDataWrapper txDataWrapper )
    {
        return updatedNodePropertyMapByKey( txDataWrapper, txDataWrapper.getTransactionData().assignedNodeProperties(), ActionType.ADDED );
    }

    public static Map<String,List<String>> removedNodePropertyMapByKey( TxDataWrapper txDataWrapper )
    {
        return updatedNodePropertyMapByKey( txDataWrapper, txDataWrapper.getTransactionData().removedNodeProperties(), ActionType.REMOVED );
    }


    public static Map<String,Map<String,List<Map<String,Object>>>> updatedNodePropertyMapByLabel( TxDataWrapper txDataWrapper,
            Iterable<PropertyEntry<Node>> entityIterable, ActionType actionType )
    {
        Map<String,Map<String,List<Map<String,Object>>>> propertyChanges = new HashMap<>();

        Iterator<PropertyEntry<Node>> entityIterator = entityIterable.iterator();
        while ( entityIterator.hasNext() )
        {
            PropertyEntry<Node> entityPropertyEntry = entityIterator.next();

            String propertyKey = entityPropertyEntry.key();
            Node updatedEntity = entityPropertyEntry.entity();

            String entityUid = txDataWrapper.getNodeUid( updatedEntity.getId() );

            List<String> labels = new ArrayList<>();

            if ( actionType.equals( ActionType.REMOVED ) )
            {
                // Get the nodes where the node properties were removed
                Node node = ((Function<Long,Node>) ( Long id ) -> StreamSupport.stream( txDataWrapper.getTransactionData().removedNodeProperties().spliterator(), true ).filter(
                        nodePropertyEntry -> id.equals( nodePropertyEntry.entity().getId() ) ).reduce( ( t1, t2 ) -> t1 ).get().entity()).apply(
                        updatedEntity.getId() );

                // Check if the node has been deleted
                if ( StreamSupport.stream( txDataWrapper.getTransactionData().deletedNodes().spliterator(), false ).map( n -> n.getId() ).collect( Collectors.toList() ).contains(
                        node.getId() ) )
                {
                    // If deleted get the labels from the transaction data.
                    labels.addAll( StreamSupport.stream( txDataWrapper.getTransactionData().removedLabels().spliterator(), false ).filter(
                            labelEntry -> labelEntry.node().getId() == node.getId() ).map( labelEntry -> labelEntry.label().name() ).collect(
                            Collectors.toList() ) );

                }
                else
                {
                    labels.addAll( StreamSupport.stream( node.getLabels().spliterator(), true ).map( label -> label.name() ).collect( Collectors.toList() ) );

                }
            }
            else
            {
                labels = StreamSupport.stream( updatedEntity.getLabels().spliterator(), true ).map( Label::name ).collect( Collectors.toList() );
            }

            for ( String label : labels )
            {
                if ( !propertyChanges.containsKey( label ) )
                {
                    propertyChanges.put( label, new HashMap<>() );
                }
                if ( !propertyChanges.get( label ).containsKey( entityUid ) )
                {
                    propertyChanges.get( label ).put( entityUid, new ArrayList<>() );
                }
                if ( actionType.equals( ActionType.REMOVED ) )
                {
                    propertyChanges.get( label ).get( entityUid ).add(
                            toMap( new PropertyChange( propertyKey, null, entityPropertyEntry.previouslyCommitedValue(), actionType ) ) );
                }
                else
                {
                    propertyChanges.get( label ).get( entityUid ).add(
                            toMap( new PropertyChange( propertyKey, entityPropertyEntry.value(), entityPropertyEntry.previouslyCommitedValue(),
                                    actionType ) ) );
                }
            }
        }

        return propertyChanges.isEmpty() ? Collections.emptyMap() : propertyChanges;
    }

    public static Map<String,Map<String,List<Map<String,Object>>>> assignedNodePropertyMapByLabel( TxDataWrapper txDataWrapper )
    {
        return updatedNodePropertyMapByLabel( txDataWrapper, txDataWrapper.getTransactionData().assignedNodeProperties(), ActionType.ADDED );
    }

    public static Map<String,Map<String,List<Map<String,Object>>>> removedNodePropertyMapByLabel( TxDataWrapper txDataWrapper )
    {
        return updatedNodePropertyMapByLabel( txDataWrapper, txDataWrapper.getTransactionData().removedNodeProperties(), ActionType.REMOVED );
    }

    public static Map<String,List<Map<String,Object>>> updatedNodePropertyMapByUid( TxDataWrapper txDataWrapper, Iterable<PropertyEntry<Node>> entityIterable, ActionType actionType )
    {
        Map<String,List<Map<String,Object>>> propertyChanges = new HashMap<>();

        Iterator<PropertyEntry<Node>> entityIterator = entityIterable.iterator();
        while ( entityIterator.hasNext() )
        {
            PropertyEntry<Node> entityPropertyEntry = entityIterator.next();

            String propertyKey = entityPropertyEntry.key();
            Node updatedEntity = entityPropertyEntry.entity();

            String entityUid = txDataWrapper.getNodeUid( updatedEntity.getId() );

            if ( !propertyChanges.containsKey( entityUid ) )
            {
                propertyChanges.put( entityUid, new ArrayList<>() );
            }
            if ( actionType.equals( ActionType.REMOVED ) )
            {
                propertyChanges.get( entityUid ).add(
                        toMap( new PropertyChange( propertyKey, null, entityPropertyEntry.previouslyCommitedValue(), actionType ) ) );
            }
            else
            {
                propertyChanges.get( entityUid ).add(
                        toMap( new PropertyChange( propertyKey, entityPropertyEntry.value(), entityPropertyEntry.previouslyCommitedValue(),
                                actionType ) ) );
            }
        }

        return propertyChanges.isEmpty() ? Collections.emptyMap() : propertyChanges;
    }

    public static Map<String,List<Map<String,Object>>> assignedNodePropertyMapByUid( TxDataWrapper txDataWrapper )
    {
        return updatedNodePropertyMapByUid( txDataWrapper, txDataWrapper.getTransactionData().assignedNodeProperties(), ActionType.ADDED );
    }

    public static Map<String,List<Map<String,Object>>> removedNodePropertyMapByUid( TxDataWrapper txDataWrapper )
    {
        return updatedNodePropertyMapByUid( txDataWrapper, txDataWrapper.getTransactionData().removedNodeProperties(), ActionType.REMOVED );
    }

    public static Map<String,List<Map<String,Object>>> updatedRelationshipPropertyMapByUid( TxDataWrapper txDataWrapper,
            Iterable<PropertyEntry<Relationship>> entityIterable, ActionType actionType )
    {
        Map<String,List<Map<String,Object>>> propertyChanges = new HashMap<>();

        Iterator<PropertyEntry<Relationship>> entityIterator = entityIterable.iterator();
        while ( entityIterator.hasNext() )
        {
            PropertyEntry<Relationship> entityPropertyEntry = entityIterator.next();

            String propertyKey = entityPropertyEntry.key();
            Relationship updatedEntity = entityPropertyEntry.entity();

            String entityUid = txDataWrapper.getRelationshipUid( updatedEntity.getId() );

            if ( !propertyChanges.containsKey( entityUid ) )
            {
                propertyChanges.put( entityUid, new ArrayList<>() );
            }
            if ( actionType.equals( ActionType.REMOVED ) )
            {
                propertyChanges.get( entityUid ).add(
                        toMap( new PropertyChange( propertyKey, null, entityPropertyEntry.previouslyCommitedValue(), actionType ) ) );
            }
            else
            {
                propertyChanges.get( entityUid ).add(
                        toMap( new PropertyChange( propertyKey, entityPropertyEntry.value(), entityPropertyEntry.previouslyCommitedValue(), actionType ) ) );
            }
        }

        return propertyChanges.isEmpty() ? Collections.emptyMap() : propertyChanges;
    }

    public static Map<String,List<Map<String,Object>>> assignedRelationshipPropertyMapByUid( TxDataWrapper txDataWrapper )
    {
        return updatedRelationshipPropertyMapByUid( txDataWrapper, txDataWrapper.getTransactionData().assignedRelationshipProperties(), ActionType.ADDED );
    }

    public static Map<String,List<Map<String,Object>>> removedRelationshipPropertyMapByUid( TxDataWrapper txDataWrapper )
    {
        return updatedRelationshipPropertyMapByUid( txDataWrapper, txDataWrapper.getTransactionData().removedRelationshipProperties(), ActionType.REMOVED );
    }

    public static Map<String,List<String>> updatedRelationshipPropertyMapByKey( TxDataWrapper txDataWrapper,
            Iterable<PropertyEntry<Relationship>> entityIterable, ActionType actionType )
    {
        Map<String,List<String>> propertyChanges = new HashMap<>();

        Iterator<PropertyEntry<Relationship>> entityIterator = entityIterable.iterator();
        while ( entityIterator.hasNext() )
        {
            PropertyEntry<Relationship> entityPropertyEntry = entityIterator.next();

            String propertyKey = entityPropertyEntry.key();
            Relationship updatedEntity = entityPropertyEntry.entity();

            String entityUid = txDataWrapper.getRelationshipUid( updatedEntity.getId() );

            if ( !propertyChanges.containsKey( propertyKey ) )
            {
                propertyChanges.put( propertyKey, new ArrayList<>() );
            }

            propertyChanges.get( propertyKey ).add( entityUid );

        }

        return propertyChanges.isEmpty() ? Collections.emptyMap() : propertyChanges;
    }

    public static Map<String,List<String>> assignedRelationshipPropertyMapByKey( TxDataWrapper txDataWrapper )
    {
        return updatedRelationshipPropertyMapByKey( txDataWrapper, txDataWrapper.getTransactionData().assignedRelationshipProperties(), ActionType.ADDED );
    }

    public static Map<String,List<String>> removedRelationshipPropertyMapByKey( TxDataWrapper txDataWrapper )
    {
        return updatedRelationshipPropertyMapByKey( txDataWrapper, txDataWrapper.getTransactionData().removedRelationshipProperties(), ActionType.REMOVED );
    }

    public static Map<String,Map<String,List<Map<String,Object>>>> updatedRelationshipPropertyMapByType( TxDataWrapper txDataWrapper,
            Iterable<PropertyEntry<Relationship>> entityIterable, ActionType actionType )
    {
        Map<String,Map<String,List<Map<String,Object>>>> propertyChanges = new HashMap<>();

        Iterator<PropertyEntry<Relationship>> entityIterator = entityIterable.iterator();
        while ( entityIterator.hasNext() )
        {
            PropertyEntry<Relationship> entityPropertyEntry = entityIterator.next();

            String propertyKey = entityPropertyEntry.key();
            Relationship updatedEntity = entityPropertyEntry.entity();

            String entityUid = txDataWrapper.getRelationshipUid( updatedEntity.getId() );

            List<String> types = new ArrayList<>();

            if ( actionType.equals( ActionType.REMOVED ) )
            {
                // Get the nodes where the node properties were removed
                Relationship relationship = ((Function<Long,Relationship>) ( Long id ) -> StreamSupport.stream( txDataWrapper.getTransactionData().removedRelationshipProperties().spliterator(), true ).filter(
                        relationshipPropertyEntry -> id.equals( relationshipPropertyEntry.entity().getId() ) ).reduce( ( t1, t2 ) -> t1 ).get().entity()).apply(
                        updatedEntity.getId() );

                // Check if the relationship has been deleted
                if ( StreamSupport.stream( txDataWrapper.getTransactionData().deletedRelationships().spliterator(), false ).map( Relationship::getId ).collect( Collectors.toList() ).contains(
                        relationship.getId() ) )
                {
                    // If deleted get the type from the transaction data. (?)
                    types.addAll(  StreamSupport.stream( txDataWrapper.getTransactionData().deletedRelationships().spliterator(), false ).filter( rel -> rel.getId() == relationship.getId() ).map( rel -> rel.getType().name() ).collect( Collectors.toList()) );
                }
                else
                {
                    types.add( relationship.getType().name() );
                }

            }
            else
            {
                types.add( updatedEntity.getType().name() );
            }

            for ( String type : types )
            {
                if ( !propertyChanges.containsKey( type ) )
                {
                    propertyChanges.put( type, new HashMap<>() );
                }
                if ( !propertyChanges.get( type ).containsKey( entityUid ) )
                {
                    propertyChanges.get( type ).put( entityUid, new ArrayList<>() );
                }
                if ( actionType.equals( ActionType.REMOVED ) )
                {
                    propertyChanges.get( type ).get( entityUid ).add(
                            toMap( new PropertyChange( propertyKey, null, entityPropertyEntry.previouslyCommitedValue(), actionType ) ) );
                }
                else
                {
                    propertyChanges.get( type ).get( entityUid ).add(
                            toMap( new PropertyChange( propertyKey, entityPropertyEntry.value(), entityPropertyEntry.previouslyCommitedValue(),
                                    actionType ) ) );
                }
            }
        }

        return propertyChanges.isEmpty() ? Collections.emptyMap() : propertyChanges;
    }

    public static Map<String,Map<String,List<Map<String,Object>>>> assignedRelationshipPropertyMapByType( TxDataWrapper txDataWrapper )
    {
        return updatedRelationshipPropertyMapByType( txDataWrapper, txDataWrapper.getTransactionData().assignedRelationshipProperties(), ActionType.ADDED );
    }

    public static Map<String,Map<String,List<Map<String,Object>>>> removedRelationshipPropertyMapByType( TxDataWrapper txDataWrapper )
    {
        return updatedRelationshipPropertyMapByType( txDataWrapper, txDataWrapper.getTransactionData().removedRelationshipProperties(), ActionType.REMOVED );
    }

    private interface TransactionDataMapObject
    {
    }

    @JsonAutoDetect
    @JsonIgnoreProperties( ignoreUnknown = true )
    public static class LabelChange implements TransactionDataMapObject
    {
        private String nodeUid;

        private ActionType action;

        public LabelChange( String nodeUid, ActionType action )
        {
            this.nodeUid = nodeUid;
            this.action = action;
        }

        public String getNodeUid()
        {
            return nodeUid;
        }

        public void setNodeUid( String nodeUid )
        {
            this.nodeUid = nodeUid;
        }

        public ActionType getAction()
        {
            return action;
        }

        public void setAction( ActionType action )
        {
            this.action = action;
        }
    }

    @JsonAutoDetect
    @JsonIgnoreProperties( ignoreUnknown = true )
    public static class NodeChange implements TransactionDataMapObject
    {
        private ActionType action;

        public NodeChange( ActionType action )
        {
            this.action = action;
        }

        public ActionType getAction()
        {
            return action;
        }

        public void setAction( ActionType action )
        {
            this.action = action;
        }
    }

    @JsonAutoDetect
    @JsonIgnoreProperties( ignoreUnknown = true )
    public static class RelationshipChange implements TransactionDataMapObject
    {
        private String uidOfStartNode;

        private String uidOfEndNode;

        private String type;

        private ActionType action;

        public RelationshipChange( String uidOfStartNode, String uidOfEndNode, String type, ActionType action )
        {
            this.uidOfStartNode = uidOfStartNode;
            this.uidOfEndNode = uidOfEndNode;
            this.type = type;
            this.action = action;
        }

        public String getUidOfStartNode()
        {
            return uidOfStartNode;
        }

        public void setUidOfStartNode( String uidOfStartNode )
        {
            this.uidOfStartNode = uidOfStartNode;
        }

        public String getUidOfEndNode()
        {
            return uidOfEndNode;
        }

        public void setUidOfEndNode( String uidOfEndNode )
        {
            this.uidOfEndNode = uidOfEndNode;
        }

        public String getType()
        {
            return type;
        }

        public void setType( String type )
        {
            this.type = type;
        }

        public ActionType getAction()
        {
            return action;
        }

        public void setAction( ActionType action )
        {
            this.action = action;
        }
    }

    @JsonAutoDetect
    @JsonIgnoreProperties( ignoreUnknown = true )
    public static class PropertyChange implements TransactionDataMapObject
    {
        private String key;

        private Object value;

        private Object oldValue;

        private String type;

        private ActionType action;

        PropertyChange( String key, Object value, Object oldValue, ActionType action )
        {
            this.key = key;
            this.value = value;
            this.type = value == null ? null : value.getClass().getSimpleName();
            if ( type != null )
            {
                if ( value instanceof long[] )
                {
                    this.type = ARRAY_PREFIX + ".Long";
                }
                else if ( value instanceof boolean[] )
                {
                    this.type = ARRAY_PREFIX + ".Boolean";
                }
                else if ( value instanceof Object[] && ((Object[]) value).length > 0)
                {
                    this.type = ARRAY_PREFIX + "." + (((Object[]) value)[0]).getClass().getSimpleName();
                }
            }
            this.action = action;
            this.oldValue = oldValue;
        }

        public String getKey()
        {
            return key;
        }

        public void setKey( String key )
        {
            this.key = key;
        }

        public Object getValue()
        {
            return value;
        }

        public void setValue( Object value )
        {
            this.value = value;
        }

        public Object getOldValue()
        {
            return oldValue;
        }

        public void setOldValue( Object oldValue )
        {
            this.oldValue = oldValue;
        }

        public String getType()
        {
            return type;
        }

        public void setType( String type )
        {
            this.type = type;
        }

        public ActionType getAction()
        {
            return action;
        }

        public void setAction( ActionType action )
        {
            this.action = action;
        }
    }

    @JsonAutoDetect
    @JsonIgnoreProperties( ignoreUnknown = true )
    public static class UniqueIdentifier
    {
        private String uidGroup = "";
        private String uidKey = "";
        private String uidValue = "";

        public UniqueIdentifier()
        {
        }

        public UniqueIdentifier( String uidValue )
        {
            this.uidValue = uidValue;
        }

        public UniqueIdentifier( String uidGroup, String uidKey, String uidValue )
        {
            this.uidGroup = uidGroup;
            this.uidKey = uidKey;
            this.uidValue = uidValue;
        }

        public String getUidGroup()
        {
            return uidGroup;
        }

        public void setUidGroup( String uidGroup )
        {
            this.uidGroup = uidGroup;
        }

        public String getUidKey()
        {
            return uidKey;
        }

        public void setUidKey( String uidKey )
        {
            this.uidKey = uidKey;
        }

        public String getUidValue()
        {
            return uidValue;
        }

        public void setUidValue( String uidValue )
        {
            this.uidValue = uidValue;
        }

        public String buildUidString()
        {
            return String.format( "`%s`:`%s`:`%s`", uidGroup, uidKey, uidValue );
        }
    }

    public static UniqueIdentifier getNodeUidFromCurrentCommit( TxDataWrapper txDataWrapper, Node node )
    {
        List<String> uidKeys = txDataWrapper.getUidKeys();
        List<String> labels = txDataWrapper.getLabels();

        String uidKeyUsed = "";
        String nodeUid = Long.toString( node.getId() );

        String labelUsed = "";

        for ( String uidKey : uidKeys )
        {
            if ( node.hasProperty( uidKey ) )
            {
                nodeUid = (String) node.getProperty( uidKey, Long.toString( node.getId() ) );
                uidKeyUsed = uidKey;
                break;
            }
        }

        for ( String label : labels )
        {
            if ( node.hasLabel( new LabelImpl( label ) ) )
            {
                labelUsed = label;
                break;
            }
        }

        // If none of the labels match then use the head label.
        if ( labelUsed.isEmpty() )
        {
            if ( node.getLabels().iterator().hasNext() )
            {
                labelUsed = node.getLabels().iterator().next().name();
            }
        }

        return new UniqueIdentifier( labelUsed, uidKeyUsed, nodeUid );
    }

    public static UniqueIdentifier getRelationshipUidFromCurrentCommit( TxDataWrapper txDataWrapper, Relationship relationship )
    {
        List<String> uidKeys = txDataWrapper.getUidKeys();

        String uidKeyUsed = "";
        String relationshipUid = Long.toString( relationship.getId() );

        for ( String uidKey : uidKeys )
        {
            if ( relationship.hasProperty( uidKey ) )
            {
                relationshipUid = (String) relationship.getProperty( uidKey, Long.toString( relationship.getId() ) );
                uidKeyUsed = uidKey;
                break;
            }
        }

        return new UniqueIdentifier( relationship.getType().name(), uidKeyUsed, relationshipUid );
    }

    public static UniqueIdentifier getNodeUidFromPreviousCommit( TxDataWrapper txDataWrapper, Long id )
    {
        Map<Long,List<PropertyEntry<Node>>> propertyEntriesCache = txDataWrapper.getRemovedNodePropertiesCache();
        Map<Long,List<Label>> removedNodeLabelsCache = txDataWrapper.getRemovedNodeLabelsCache();

        List<String> uidKeys = txDataWrapper.getUidKeys();
        List<String> labels = txDataWrapper.getLabels();

        String uidKeyUsed = "";
        String uidValue = Long.toString( id );

        String labelUsed = "";

        boolean breakOuter = false;

        for ( String uidKey : uidKeys )
        {
            for ( PropertyEntry<Node> p : propertyEntriesCache.getOrDefault( id, Collections.emptyList() ) )
            {
                if ( p.key().equals( uidKey ) )
                {
                    uidKeyUsed = uidKey;
                    uidValue = (String) p.previouslyCommitedValue();
                    breakOuter = true;
                    break;
                }
            }
            if ( breakOuter )
            {
                break;
            }
        }

        breakOuter = false;

        for ( String label : labels )
        {
            for ( Label l : removedNodeLabelsCache.getOrDefault( id, Collections.emptyList() ) )
            {
                if ( l.name().equals( label ) )
                {
                    labelUsed = label;
                    breakOuter = true;
                    break;
                }
            }
            if ( breakOuter )
            {
                break;
            }
        }

        // If labelUsed still empty then just use the head.
        if ( labelUsed.isEmpty() )
        {
            for ( Label l : removedNodeLabelsCache.getOrDefault( id, Collections.emptyList() ) )
            {
                labelUsed = l.name();
                break;
            }
        }

        return new UniqueIdentifier( labelUsed, uidKeyUsed, uidValue );
    }

    public static UniqueIdentifier getRelationshipUidFromPreviousCommit( TxDataWrapper txDataWrapper, Long id )
    {
        Map<Long,List<PropertyEntry<Relationship>>> propertyEntriesCache = txDataWrapper.getRemovedRelationshipPropertiesCache();

        List<String> uidKeys = txDataWrapper.getUidKeys();

        String uidKeyUsed = "";
        String uidValue = Long.toString( id );

        String typeUsed = "";

        boolean breakOuter = false;

        for (String uidKey : uidKeys )
        {
            for ( PropertyEntry<Relationship> p : propertyEntriesCache.getOrDefault( id, Collections.emptyList() ) )
            {
                if ( p.key().equals( uidKey ) )
                {
                    uidKeyUsed = uidKey;
                    uidValue = (String) p.previouslyCommitedValue();
                    breakOuter = true;

                    typeUsed = p.entity().getType().name();
                    break;
                }
            }
            if (breakOuter)
            {
                break;
            }
        }

        return new UniqueIdentifier( typeUsed, uidKeyUsed, uidValue );
    }

    public static class TxDataWrapper
    {
        private final TransactionData transactionData;

        private final Map<Long,List<PropertyEntry<Node>>> assignedNodePropertiesCache;
        private final Map<Long,List<PropertyEntry<Node>>> removedNodePropertiesCache;

        private final Map<Long,List<PropertyEntry<Relationship>>> assignedRelationshipPropertiesCache;
        private final Map<Long,List<PropertyEntry<Relationship>>> removedRelationshipPropertiesCache;

        private final Map<Long,List<Label>> assignedNodeLabelsCache;
        private final Map<Long,List<Label>> removedNodeLabelsCache;

        private final List<String> uidKeys;
        private final List<String> labels;

        private final Map<Long, Node> deletedNodesMap;
        private final Map<Long, Node> createdNodesMap;
        private final Map<Long, Node> updatedNodesMap;
        private final Set<Long> allNodes;

        private final Map<Long, Relationship> deletedRelationshipsMap;
        private final Map<Long, Relationship> createdRelationshipsMap;
        private final Map<Long, Relationship> updatedRelationshipsMap;
        private final Set<Long> allRelationships;

        private final Map<Long,String> nodeIdToUid = new HashMap<>();
        private final Map<Long,String> relationshipIdToUid = new HashMap<>();


        public TxDataWrapper(TransactionData transactionData, List<String> uidKeys, List<String> labels)
        {
            this.transactionData = transactionData;
            this.uidKeys = uidKeys;
            this.labels = labels;

            // Nodes Setup
            assignedNodePropertiesCache = StreamSupport.stream( transactionData.assignedNodeProperties().spliterator(), true ).collect( Collectors.groupingBy( x -> x.entity().getId() ) );
            removedNodePropertiesCache = StreamSupport.stream( transactionData.removedNodeProperties().spliterator(), true ).collect( Collectors.groupingBy( x -> x.entity().getId() ) );

            assignedNodeLabelsCache = StreamSupport.stream( transactionData.assignedLabels().spliterator(), true ).collect( Collectors.groupingBy( x -> x.node().getId(), Collectors.mapping( labelEntry -> labelEntry.label(), Collectors.toList() ) ) );
            removedNodeLabelsCache = StreamSupport.stream( transactionData.removedLabels().spliterator(), true ).collect( Collectors.groupingBy( x -> x.node().getId(), Collectors.mapping( labelEntry -> labelEntry.label(), Collectors.toList() ) ) );

            deletedNodesMap = StreamSupport.stream( transactionData.deletedNodes().spliterator(), false ).collect( Collectors.toMap( Node::getId, x -> x ) );
            createdNodesMap = StreamSupport.stream( transactionData.createdNodes().spliterator(), false ).collect( Collectors.toMap( Node::getId, x -> x ) );


            List<Node> updatedRemovedPropertiesNodes = StreamSupport.stream( transactionData.removedNodeProperties().spliterator(), false ).map( PropertyEntry::entity ).filter(
                    n -> !deletedNodesMap.keySet().contains( n.getId() ) && !createdNodesMap.keySet().contains( n.getId() ) ).collect( Collectors.toList() );

            List<Node> updatedAssignedPropertiesNodes = StreamSupport.stream( transactionData.assignedNodeProperties().spliterator(), false ).map( PropertyEntry::entity ).filter(
                    n -> !deletedNodesMap.keySet().contains( n.getId() ) && !createdNodesMap.keySet().contains( n.getId() ) ).collect( Collectors.toList() );

            List<Node> updatedRemovedLabelsNodes = StreamSupport.stream( transactionData.removedLabels().spliterator(), false ).map( LabelEntry::node ).filter(
                    n -> !deletedNodesMap.keySet().contains( n.getId() ) && !createdNodesMap.keySet().contains( n.getId() ) ).collect( Collectors.toList() );

            List<Node> updatedAssignedLabelsNodes = StreamSupport.stream( transactionData.assignedLabels().spliterator(), false ).map( LabelEntry::node ).filter(
                    n -> !deletedNodesMap.keySet().contains( n.getId() ) && !createdNodesMap.keySet().contains( n.getId() ) ).collect( Collectors.toList() );

            updatedNodesMap = Stream.of(updatedRemovedPropertiesNodes, updatedAssignedPropertiesNodes, updatedRemovedLabelsNodes, updatedAssignedLabelsNodes)
                    .flatMap( Collection::stream)
                    .collect( Collectors.toSet() )
                    .stream()
                    .collect(Collectors.toMap( Node::getId, n->n));


            allNodes = Stream.of(createdNodesMap.keySet(), deletedNodesMap.keySet(), updatedNodesMap.keySet())
                    .flatMap( Collection::stream)
                    .collect(Collectors.toSet());

            for (Long id : allNodes)
            {
                nodeIdToUid.put( id, calculateNodeUID( this, id ).buildUidString() );
            }


            // Relationships Setup
            assignedRelationshipPropertiesCache = StreamSupport.stream( transactionData.assignedRelationshipProperties().spliterator(), true ).collect( Collectors.groupingBy( x -> x.entity().getId() ) );
            removedRelationshipPropertiesCache = StreamSupport.stream( transactionData.removedRelationshipProperties().spliterator(), true ).collect( Collectors.groupingBy( x -> x.entity().getId() ) );

            createdRelationshipsMap = StreamSupport.stream( transactionData.createdRelationships().spliterator(), false ).collect( Collectors.toMap( Relationship::getId, x -> x ) );
            deletedRelationshipsMap = StreamSupport.stream( transactionData.deletedRelationships().spliterator(), false ).collect( Collectors.toMap( Relationship::getId, x -> x ) );

            List<Relationship> updatedRemovedPropertiesRelationships = StreamSupport.stream( transactionData.removedRelationshipProperties().spliterator(), false ).map( PropertyEntry::entity ).filter(
                    n -> !createdRelationshipsMap.keySet().contains( n.getId() ) && !deletedRelationshipsMap.keySet().contains( n.getId() ) ).collect( Collectors.toList() );

            List<Relationship> updatedAssignedPropertiesRelationships = StreamSupport.stream( transactionData.assignedRelationshipProperties().spliterator(), false ).map( PropertyEntry::entity ).filter(
                    n -> !createdRelationshipsMap.keySet().contains( n.getId() ) && !deletedRelationshipsMap.keySet().contains( n.getId() ) ).collect( Collectors.toList() );

            updatedRelationshipsMap = Stream.of(updatedRemovedPropertiesRelationships, updatedAssignedPropertiesRelationships )
                    .flatMap( Collection::stream)
                    .collect( Collectors.toSet() )
                    .stream()
                    .collect(Collectors.toMap( Relationship::getId, n->n));

            allRelationships = Stream.of(createdRelationshipsMap.keySet(), deletedRelationshipsMap.keySet(), updatedRelationshipsMap.keySet())
                    .flatMap( Collection::stream)
                    .collect(Collectors.toSet());

            for (Long id : allRelationships)
            {
                relationshipIdToUid.put( id, calculateRelationshipUID( this, id ).buildUidString() );
            }

        }

        public String getNodeUid( Long id )
        {
            return nodeIdToUid.get( id );
        }

        public String getRelationshipUid( Long id )
        {
            return relationshipIdToUid.get( id );
        }

        public TransactionData getTransactionData()
        {
            return transactionData;
        }

        public Map<Long,List<PropertyEntry<Node>>> getRemovedNodePropertiesCache()
        {
            return removedNodePropertiesCache;
        }

        public Map<Long,List<PropertyEntry<Relationship>>> getRemovedRelationshipPropertiesCache()
        {
            return removedRelationshipPropertiesCache;
        }

        public Map<Long,List<Label>> getRemovedNodeLabelsCache()
        {
            return removedNodeLabelsCache;
        }

        public List<String> getUidKeys()
        {
            return uidKeys;
        }

        public List<String> getLabels()
        {
            return labels;
        }

        public Map<Long,Node> getDeletedNodesMap()
        {
            return deletedNodesMap;
        }

        public Map<Long,Node> getCreatedNodesMap()
        {
            return createdNodesMap;
        }

        public Map<Long,Node> getUpdatedNodesMap()
        {
            return updatedNodesMap;
        }

        public boolean isNodeDeleted( Long id )
        {
            return deletedNodesMap.containsKey( id );
        }

        public boolean isNodeCreated( Long id )
        {
            return createdNodesMap.containsKey( id );
        }

        public boolean isNodeUpdated( Long id )
        {
            return updatedNodesMap.containsKey( id );
        }

        public boolean isRelationshipDeleted( Long id )
        {
            return deletedRelationshipsMap.containsKey( id );
        }

        public boolean isRelationshipCreated( Long id )
        {
            return createdRelationshipsMap.containsKey( id );
        }

        public boolean isRelationshipUpdated( Long id )
        {
            return updatedRelationshipsMap.containsKey( id );
        }

        public Map<Long,List<Label>> getAssignedNodeLabelsCache()
        {
            return assignedNodeLabelsCache;
        }

        public Map<Long,List<PropertyEntry<Node>>> getAssignedNodePropertiesCache()
        {
            return assignedNodePropertiesCache;
        }

        public Map<Long,String> getNodeIdToUid()
        {
            return nodeIdToUid;
        }

        public Map<Long,List<PropertyEntry<Relationship>>> getAssignedRelationshipPropertiesCache()
        {
            return assignedRelationshipPropertiesCache;
        }

        public Set<Long> getAllNodes()
        {
            return allNodes;
        }

        public Set<Long> getAllRelationships()
        {
            return allRelationships;
        }

        public Map<Long,Relationship> getDeletedRelationshipsMap()
        {
            return deletedRelationshipsMap;
        }

        public Map<Long,Relationship> getCreatedRelationshipsMap()
        {
            return createdRelationshipsMap;
        }

        public Map<Long,Relationship> getUpdatedRelationshipsMap()
        {
            return updatedRelationshipsMap;
        }

        public Map<Long,String> getRelationshipIdToUid()
        {
            return relationshipIdToUid;
        }
    }

    private static class LabelImpl implements Label
    {
        private String name;

        public LabelImpl( String name )
        {
            this.name = name;
        }

        @Override
        public String name()
        {
            return name;
        }
    }

    public static UniqueIdentifier calculateNodeUID( TxDataWrapper txDataWrapper, Long id )
    {

        if ( txDataWrapper.isNodeCreated( id ) )
        {
            return getNodeUidFromCurrentCommit( txDataWrapper, txDataWrapper.getCreatedNodesMap().get( id ) );
        }

        if ( txDataWrapper.isNodeDeleted( id ) )
        {
            return getNodeUidFromPreviousCommit( txDataWrapper, id );
        }

        // Must be updated.
        if ( !txDataWrapper.isNodeUpdated( id ) )
        {
            // Should never get here.
        }

        Node updatedNode = txDataWrapper.getUpdatedNodesMap().get( id );

        // Labels, using previous commit so
            // CAN use removed labels in labelList
            // CAN'T use added labels in labelList
            // CAN use unaffected labels in labelList

        String labelUsed = "";
        List<String> labelList = txDataWrapper.getLabels();

        List<Label> allowableLabels = new ArrayList<>( txDataWrapper.getRemovedNodeLabelsCache().getOrDefault( id, new ArrayList<>() ) );
        List<Label> restrictedLabels = new ArrayList<>( txDataWrapper.getAssignedNodeLabelsCache().getOrDefault( id, Collections.emptyList() ) );

        // Allow any labels that has previously been on the node.
        allowableLabels.addAll(
                StreamSupport.stream( updatedNode.getLabels().spliterator(), false ).filter( label -> !restrictedLabels.contains( label ) ).collect(
                        Collectors.toList() ) );

        boolean breakOuter = false;
        // Check if any of the labels in the labelList are on the node
        for ( String label : labelList )
        {
            for ( Label l : allowableLabels )
            {
                if ( l.name().equals( label ) )
                {
                    labelUsed = label;
                    breakOuter = true;
                    break;
                }
            }
            if ( breakOuter )
            {
                break;
            }
        }

        // If labelUsed still empty then just use the head.
        if ( labelUsed.isEmpty() && !allowableLabels.isEmpty() )
        {
            labelUsed = allowableLabels.get( 0 ).name();
        }
        // By this point labelUsed (uidGroup) has been set!

        // Moving on to setting the uidKey and uidValue
        List<String> uidKeys = txDataWrapper.getUidKeys();

        String uidKeyUsed = "";
        // Default uidValue to the id
        String uidValue = Long.toString( id );



        // Properties, using previous commit so
            // CAN use removed properties
            // CAN'T use newly added properties
            // NEED to use OLD VALUE of any updated properties
            // Can use unaffected properties

        // Properties currently on the node.
        List<String> currentProperties = StreamSupport.stream( updatedNode.getPropertyKeys().spliterator(), false ).collect( Collectors.toList() );

        // Newly added properties
        List<String> newProperties = txDataWrapper.getAssignedNodePropertiesCache().getOrDefault( id, Collections.emptyList() ).stream().filter(
                pe -> pe.previouslyCommitedValue() == null ).map( PropertyEntry::key ).collect( Collectors.toList() );


        // Properties previously on the node that were updated
        Map<String,PropertyEntry<Node>> updatedProperties =
                txDataWrapper.getAssignedNodePropertiesCache().getOrDefault( id, Collections.emptyList() ).stream().filter(
                        pe -> !newProperties.contains( pe.key() ) ).collect( Collectors.toMap( PropertyEntry::key, x -> x ) );

        // Properties previously on the node that were removed
        Map<String,PropertyEntry<Node>> removedProperties =  txDataWrapper.getRemovedNodePropertiesCache().getOrDefault( id, Collections.emptyList() ).stream().collect( Collectors.toMap( PropertyEntry::key, x->x) );

        List<String> allowableProperties = new ArrayList<>();
        allowableProperties.addAll( updatedProperties.keySet() );
        allowableProperties.addAll( removedProperties.keySet() );
        allowableProperties.addAll( currentProperties.stream().filter( propKey -> !newProperties.contains( propKey ) ).collect( Collectors.toList() ) );

        breakOuter = false;
        for ( String uidKey : uidKeys )
        {
            for ( String propKey : allowableProperties )
            {
                if ( propKey.equals( uidKey ) )
                {
                    if ( updatedProperties.containsKey( propKey ) )
                    {
                        uidKeyUsed = propKey;
                        uidValue = (String) updatedProperties.get( propKey ).previouslyCommitedValue();
                        breakOuter = true;
                        break;
                    }
                    else if ( removedProperties.containsKey( propKey ) )
                    {
                        uidKeyUsed = propKey;
                        uidValue = (String) removedProperties.get( propKey ).previouslyCommitedValue();
                        breakOuter = true;
                        break;
                    }
                    else if ( currentProperties.contains( propKey ) )
                    {
                        uidKeyUsed = propKey;
                        uidValue = (String) updatedNode.getProperty( propKey );
                        breakOuter = true;
                        break;
                    }
                }
            }
            if ( breakOuter )
            {
                break;
            }
        }

        return new UniqueIdentifier( labelUsed, uidKeyUsed, uidValue );
    }

    public static UniqueIdentifier calculateRelationshipUID( TxDataWrapper txDataWrapper, Long id )
    {

        if ( txDataWrapper.isRelationshipCreated( id ) )
        {
            return getRelationshipUidFromCurrentCommit( txDataWrapper, txDataWrapper.getCreatedRelationshipsMap().get( id ) );
        }

        if ( txDataWrapper.isRelationshipDeleted( id ) )
        {
            return getRelationshipUidFromPreviousCommit( txDataWrapper, id );
        }

        // Must be updated.
        if ( !txDataWrapper.isRelationshipUpdated( id ) )
        {
            // Should never get here.
        }

        Relationship updatedRelationship = txDataWrapper.getUpdatedRelationshipsMap().get( id );

        // Relationship Type is Immutable
        String typeUsed = updatedRelationship.getType().name();


        // Moving on to setting the uidKey and uidValue
        List<String> uidKeys = txDataWrapper.getUidKeys();

        String uidKeyUsed = "";
        // Default uidValue to the id
        String uidValue = Long.toString( id );

        // Properties, using previous commit so
            // CAN use removed properties
            // CAN'T use newly added properties
            // NEED to use OLD VALUE of any updated properties
            // Can use unaffected properties

        // Properties currently on the node.
        List<String> currentProperties = StreamSupport.stream( updatedRelationship.getPropertyKeys().spliterator(), false ).collect( Collectors.toList() );

        // Newly added properties
        List<String> newProperties = txDataWrapper.getAssignedRelationshipPropertiesCache().getOrDefault( id, Collections.emptyList() ).stream().filter(
                pe -> pe.previouslyCommitedValue() == null ).map( PropertyEntry::key ).collect( Collectors.toList() );


        // Properties previously on the node that were updated
        Map<String,PropertyEntry<Relationship>> updatedProperties =
                txDataWrapper.getAssignedRelationshipPropertiesCache().getOrDefault( id, Collections.emptyList() ).stream().filter(
                        pe -> !newProperties.contains( pe.key() ) ).collect( Collectors.toMap( PropertyEntry::key, x -> x ) );

        // Properties previously on the node that were removed
        Map<String,PropertyEntry<Relationship>> removedProperties =  txDataWrapper.getRemovedRelationshipPropertiesCache().getOrDefault( id, Collections.emptyList() ).stream().collect( Collectors.toMap( PropertyEntry::key, x->x) );

        List<String> allowableProperties = new ArrayList<>();
        allowableProperties.addAll( updatedProperties.keySet() );
        allowableProperties.addAll( removedProperties.keySet() );
        allowableProperties.addAll( currentProperties.stream().filter( propKey -> !newProperties.contains( propKey ) ).collect( Collectors.toList() ) );

        boolean breakOuter = false;
        for ( String uidKey : uidKeys )
        {
            for ( String propKey : allowableProperties )
            {
                if ( propKey.equals( uidKey ) )
                {
                    if ( updatedProperties.containsKey( propKey ) )
                    {
                        uidKeyUsed = propKey;
                        uidValue = (String) updatedProperties.get( propKey ).previouslyCommitedValue();
                        breakOuter = true;
                        break;
                    }
                    else if ( removedProperties.containsKey( propKey ) )
                    {
                        uidKeyUsed = propKey;
                        uidValue = (String) removedProperties.get( propKey ).previouslyCommitedValue();
                        breakOuter = true;
                        break;
                    }
                    else if ( currentProperties.contains( propKey ) )
                    {
                        uidKeyUsed = propKey;
                        uidValue = (String) updatedRelationship.getProperty( propKey );
                        breakOuter = true;
                        break;
                    }
                }
            }
            if ( breakOuter )
            {
                break;
            }
        }

        return new UniqueIdentifier( typeUsed, uidKeyUsed, uidValue );
    }
}
