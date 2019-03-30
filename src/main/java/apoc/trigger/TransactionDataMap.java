package apoc.trigger;

import apoc.util.JsonUtil;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
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



    public static Map<String,Map<String,Object>> updatedNodeMap( TransactionData tx, Iterable<Node> updatedNodes, String uidKey, ActionType actionType )
    {
        Map<String,Map<String,Object>> nodeChanges = new HashMap<>();

        Iterator<Node> updatedNodesIterator = updatedNodes.iterator();
        while ( updatedNodesIterator.hasNext() )
        {
            Node node = updatedNodesIterator.next();
            String nodeUid = Long.toString( node.getId() );

            switch ( actionType )
            {
            case ADDED:
                nodeUid = (String) node.getProperty( uidKey, Long.toString( node.getId() ) );
                break;
            case REMOVED:
                nodeUid = getNodeUidFromPreviousCommit( tx, uidKey, node.getId() );
                break;
            }

            nodeChanges.put( nodeUid, toMap( new NodeChange( actionType ) ) );
        }

        return nodeChanges.isEmpty() ? Collections.emptyMap() : nodeChanges;
    }

    public static Map<String,Map<String,Object>> createdNodeMap( TransactionData tx, String uidKey )
    {
        return updatedNodeMap( tx, tx.createdNodes(), uidKey, ActionType.ADDED );
    }

    public static Map<String,Map<String,Object>> deletedNodeMap( TransactionData tx, String uidKey )
    {
        return updatedNodeMap( tx, tx.deletedNodes(), uidKey, ActionType.REMOVED );
    }

    public static Map<String,Map<String,Object>> updatedRelationshipsMap( TransactionData tx, Iterable<Relationship> updatedRelationships, String uidKey,
            ActionType actionType )
    {
        Map<String,Map<String,Object>> relationshipChanges = new HashMap<>();

        Iterator<Relationship> updatedRelationshipsIterator = updatedRelationships.iterator();
        while ( updatedRelationshipsIterator.hasNext() )
        {
            Relationship relationship = updatedRelationshipsIterator.next();

            Node startNode = relationship.getStartNode();
            Node endNode = relationship.getEndNode();

            String relationshipUid = Long.toString( relationship.getId() );
            String startNodeUid = Long.toString( relationship.getId() );
            String endNodeUid = Long.toString( relationship.getId() );

            switch ( actionType )
            {
            case ADDED:
                relationshipUid = (String) relationship.getProperty( uidKey, Long.toString( relationship.getId() ) );
                break;
            case REMOVED:
                relationshipUid = getRelationshipUidFromPreviousCommit( tx, uidKey, relationship.getId() );
                break;
            }

            // Check if the start node has been deleted
            if ( StreamSupport.stream( tx.deletedNodes().spliterator(), false ).map( Node::getId ).collect( Collectors.toList() ).contains(
                    startNode.getId() ) )
            {
                // Also if deleted get the old value of uidKey
                startNodeUid = getNodeUidFromPreviousCommit( tx, uidKey, startNode.getId() );
            }
            else
            {
                // If not deleted set the entityUid based which property was removed
                startNodeUid = (String) startNode.getProperty( uidKey, Long.toString( startNode.getId() ) );
            }

            // Check if the end node has been deleted
            if ( StreamSupport.stream( tx.deletedNodes().spliterator(), false ).map( Node::getId ).collect( Collectors.toList() ).contains(
                    endNode.getId() ) )
            {
                // Also if deleted get the old value of uidKey
                endNodeUid = getNodeUidFromPreviousCommit( tx, uidKey, endNode.getId() );
            }
            else
            {
                // If not deleted set the entityUid based which property was removed
                endNodeUid = (String) endNode.getProperty( uidKey, Long.toString( endNode.getId() ) );
            }

            relationshipChanges.put( relationshipUid, toMap( new RelationshipChange( startNodeUid, endNodeUid, relationship.getType().name(), actionType ) ) );
        }

        return relationshipChanges.isEmpty() ? Collections.emptyMap() : relationshipChanges;
    }

    public static Map<String,Map<String,Object>> createdRelationshipsMap( TransactionData tx, String uidKey )
    {
        return updatedRelationshipsMap( tx, tx.createdRelationships(), uidKey, ActionType.ADDED );
    }

    public static Map<String,Map<String,Object>> deletedRelationshipsMap( TransactionData tx, String uidKey )
    {
        return updatedRelationshipsMap( tx, tx.deletedRelationships(), uidKey, ActionType.REMOVED );
    }

    public static Map<String,List<Map<String,Object>>> updatedLabelMapByLabel( TransactionData tx, Iterable<LabelEntry> assignedLabels, String uidKey,
            ActionType actionType )
    {
        Map<String,List<Map<String,Object>>> labelChanges = new HashMap<>();

        Iterator<LabelEntry> assignedLabelsIterator = assignedLabels.iterator();
        while ( assignedLabelsIterator.hasNext() )
        {
            LabelEntry labelEntry = assignedLabelsIterator.next();
            Node updatedNode = labelEntry.node();

            String label = labelEntry.label().name();
            String nodeUid = Long.toString( updatedNode.getId() );

            switch ( actionType )
            {
            case ADDED:
                nodeUid = (String) updatedNode.getProperty( uidKey, Long.toString( updatedNode.getId() ) );
                break;
            case REMOVED:
                // Check if the start node has been deleted
                if ( StreamSupport.stream( tx.deletedNodes().spliterator(), false ).map( Node::getId ).collect( Collectors.toList() ).contains(
                        updatedNode.getId() ) )
                {
                    // Also if deleted get the old value of uidKey
                    nodeUid = getNodeUidFromPreviousCommit( tx, uidKey, updatedNode.getId() );
                }
                else
                {
                    // If not deleted set the entityUid based which property was removed
                    nodeUid = (String) updatedNode.getProperty( uidKey, Long.toString( updatedNode.getId() ) );
                }
                break;
            }

            if ( !labelChanges.containsKey( label ) )
            {
                labelChanges.put( label, new ArrayList<>() );
            }
            labelChanges.get( label ).add( toMap( new LabelChange( nodeUid, actionType ) ) );
        }

        return labelChanges.isEmpty() ? Collections.emptyMap() : labelChanges;
    }

    public static Map<String,List<Map<String,Object>>> assignedLabelMapByLabel( TransactionData tx, String uidKey )
    {
        return updatedLabelMapByLabel( tx, tx.assignedLabels(), uidKey, ActionType.ADDED );
    }

    public static Map<String,List<Map<String,Object>>> removedLabelMapByLabel( TransactionData tx, String uidKey )
    {
        return updatedLabelMapByLabel( tx, tx.removedLabels(), uidKey, ActionType.REMOVED );
    }

    public static Map<String,List<String>> updatedLabelMapByUid( TransactionData tx, Iterable<LabelEntry> assignedLabels, String uidKey,
            ActionType actionType )
    {
        Map<String,List<String>> uidTolabelsMap = new HashMap<>();

        Iterator<LabelEntry> assignedLabelsIterator = assignedLabels.iterator();
        while ( assignedLabelsIterator.hasNext() )
        {
            LabelEntry labelEntry = assignedLabelsIterator.next();
            Node updatedNode = labelEntry.node();

            String label = labelEntry.label().name();
            String nodeUid = Long.toString( updatedNode.getId() );

            switch ( actionType )
            {
            case ADDED:
                nodeUid = (String) updatedNode.getProperty( uidKey, Long.toString( updatedNode.getId() ) );
                break;
            case REMOVED:
                // Check if the start node has been deleted
                if ( StreamSupport.stream( tx.deletedNodes().spliterator(), false ).map( Node::getId ).collect( Collectors.toList() ).contains(
                        updatedNode.getId() ) )
                {
                    // Also if deleted get the old value of uidKey
                    nodeUid = getNodeUidFromPreviousCommit( tx, uidKey, updatedNode.getId() );
                }
                else
                {
                    // If not deleted set the entityUid based which property was removed
                    nodeUid = (String) updatedNode.getProperty( uidKey, Long.toString( updatedNode.getId() ) );
                }
                break;
            }

            if ( !uidTolabelsMap.containsKey( nodeUid ) )
            {
                uidTolabelsMap.put( nodeUid, new ArrayList<>() );
            }
            uidTolabelsMap.get( nodeUid ).add( label );
        }

        return uidTolabelsMap.isEmpty() ? Collections.emptyMap() : uidTolabelsMap;
    }

    public static Map<String,List<String>> assignedLabelMapByUid( TransactionData tx, String uidKey )
    {
        return updatedLabelMapByUid( tx, tx.assignedLabels(), uidKey, ActionType.ADDED );
    }

    public static Map<String,List<String>> removedLabelMapByUid( TransactionData tx, String uidKey )
    {
        return updatedLabelMapByUid( tx, tx.removedLabels(), uidKey, ActionType.REMOVED );
    }

    /**
     * Returns the updated node properties by key (property name) and value (list of node uids).
     * @param tx
     * @param entityIterable
     * @param uidKey
     * @param actionType
     * @return
     */
    public static Map<String,List<String>> updatedNodePropertyMapByKey( TransactionData tx,
            Iterable<PropertyEntry<Node>> entityIterable, String uidKey, ActionType actionType )
    {
        Map<String,List<String>> propertyChanges = new HashMap<>();

        Iterator<PropertyEntry<Node>> entityIterator = entityIterable.iterator();
        while ( entityIterator.hasNext() )
        {
            PropertyEntry<Node> entityPropertyEntry = entityIterator.next();

            String propertyKey = entityPropertyEntry.key();

            Node updatedEntity = entityPropertyEntry.entity();

            String entityUid = Long.toString( updatedEntity.getId() );

            // Check if the node has been deleted
            if ( StreamSupport.stream( tx.deletedNodes().spliterator(), false ).map( n -> n.getId() ).collect( Collectors.toList() ).contains(
                    updatedEntity.getId() ) )
            {
                // If deleted get the old value of uidKey
                entityUid = getNodeUidFromPreviousCommit( tx, uidKey, updatedEntity.getId() );
            }
            else
            {
                // If not deleted set the entityUid based which property was removed
                entityUid = (actionType.equals( ActionType.REMOVED ) && propertyKey.equals( uidKey ) ) ? getNodeUidFromPreviousCommit( tx, uidKey, updatedEntity.getId() )
                                                                                                       : (String) updatedEntity.getProperty( uidKey, Long.toString( updatedEntity.getId() ) );
            }

            if (!propertyChanges.containsKey( propertyKey ))
            {
                propertyChanges.put( propertyKey, new ArrayList<>(  ) );
            }

            propertyChanges.get( propertyKey ).add( entityUid );
        }

        return propertyChanges.isEmpty() ? Collections.emptyMap() : propertyChanges;
    }

    public static Map<String,List<String>> assignedNodePropertyMapByKey( TransactionData tx, String uidKey )
    {
        return updatedNodePropertyMapByKey( tx, tx.assignedNodeProperties(), uidKey, ActionType.ADDED );
    }

    public static Map<String,List<String>> removedNodePropertyMapByKey( TransactionData tx, String uidKey )
    {
        return updatedNodePropertyMapByKey( tx, tx.removedNodeProperties(), uidKey, ActionType.REMOVED );
    }


    public static Map<String,Map<String,List<Map<String,Object>>>> updatedNodePropertyMapByLabel( TransactionData tx,
            Iterable<PropertyEntry<Node>> entityIterable, String uidKey, ActionType actionType, Function<Long,String> uidFunction )
    {
        Map<String,Map<String,List<Map<String,Object>>>> propertyChanges = new HashMap<>();

        Iterator<PropertyEntry<Node>> entityIterator = entityIterable.iterator();
        while ( entityIterator.hasNext() )
        {
            PropertyEntry<Node> entityPropertyEntry = entityIterator.next();

            String propertyKey = entityPropertyEntry.key();
            Node updatedEntity = entityPropertyEntry.entity();

            String entityUid = Long.toString( updatedEntity.getId() );

            List<String> labels = new ArrayList<>();

            if ( actionType.equals( ActionType.REMOVED ) )
            {
                // Get the nodes where the node properties were removed
                Node node = ((Function<Long,Node>) ( Long id ) -> StreamSupport.stream( tx.removedNodeProperties().spliterator(), true ).filter(
                        nodePropertyEntry -> id.equals( nodePropertyEntry.entity().getId() ) ).reduce( ( t1, t2 ) -> t1 ).get().entity()).apply(
                        updatedEntity.getId() );

                // Check if the node has been deleted
                if ( StreamSupport.stream( tx.deletedNodes().spliterator(), false ).map( n -> n.getId() ).collect( Collectors.toList() ).contains(
                        node.getId() ) )
                {
                    // If deleted get the labels from the transaction data.
                    labels.addAll( StreamSupport.stream( tx.removedLabels().spliterator(), false ).filter(
                            labelEntry -> labelEntry.node().getId() == node.getId() ).map( labelEntry -> labelEntry.label().name() ).collect(
                            Collectors.toList() ) );

                    // Also if deleted get the old value of uidKey
                    entityUid = uidFunction.apply( updatedEntity.getId() );
                }
                else
                {
                    labels.addAll( StreamSupport.stream( node.getLabels().spliterator(), true ).map( label -> label.name() ).collect( Collectors.toList() ) );

                    // If not deleted set the entityUid based which property was removed
                    entityUid = propertyKey.equals( uidKey ) ? uidFunction.apply( updatedEntity.getId() ) : (String) updatedEntity.getProperty( uidKey, Long.toString( updatedEntity.getId() ) );
                }
            }
            else
            {
                labels = StreamSupport.stream( updatedEntity.getLabels().spliterator(), true ).map( Label::name ).collect( Collectors.toList() );
                entityUid = (String) updatedEntity.getProperty( uidKey, Long.toString( updatedEntity.getId() ) );
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

    public static Map<String,Map<String,List<Map<String,Object>>>> assignedNodePropertyMapByLabel( TransactionData tx, String uidKey )
    {
        return updatedNodePropertyMapByLabel( tx, tx.assignedNodeProperties(), uidKey, ActionType.ADDED,
                ( l ) -> getNodeUidFromPreviousCommit( tx, uidKey, l ) );
    }

    public static Map<String,Map<String,List<Map<String,Object>>>> removedNodePropertyMapByLabel( TransactionData tx, String uidKey )
    {
        return updatedNodePropertyMapByLabel( tx, tx.removedNodeProperties(), uidKey, ActionType.REMOVED,
                ( l ) -> getNodeUidFromPreviousCommit( tx, uidKey, l ) );
    }

    public static Map<String,List<Map<String,Object>>> updatedNodePropertyMapByUid( TransactionData tx,
            Iterable<PropertyEntry<Node>> entityIterable, String uidKey, ActionType actionType, Function<Long,String> uidFunction )
    {
        Map<String,List<Map<String,Object>>> propertyChanges = new HashMap<>();

        Iterator<PropertyEntry<Node>> entityIterator = entityIterable.iterator();
        while ( entityIterator.hasNext() )
        {
            PropertyEntry<Node> entityPropertyEntry = entityIterator.next();

            String propertyKey = entityPropertyEntry.key();
            Node updatedEntity = entityPropertyEntry.entity();

            String entityUid = Long.toString( updatedEntity.getId() );

            // Check if the node has been deleted
            if ( StreamSupport.stream( tx.deletedNodes().spliterator(), false ).map( n -> n.getId() ).collect( Collectors.toList() ).contains(
                    updatedEntity.getId() ) )
            {
                // If deleted get the old value of uidKey
                entityUid = uidFunction.apply( updatedEntity.getId() );
            }
            else
            {
                // If not deleted set the entityUid based which property was removed
                entityUid = (actionType.equals( ActionType.REMOVED ) && propertyKey.equals( uidKey ) ) ? uidFunction.apply( updatedEntity.getId() )
                                                                                                       : (String) updatedEntity.getProperty( uidKey, Long.toString( updatedEntity.getId() ) );
            }

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
    public static Map<String,List<Map<String,Object>>> updatedRelationshipPropertyMapByUid( TransactionData tx,
            Iterable<PropertyEntry<Relationship>> entityIterable, String uidKey, ActionType actionType, Function<Long,String> uidFunction )
    {
        Map<String,List<Map<String,Object>>> propertyChanges = new HashMap<>();

        Iterator<PropertyEntry<Relationship>> entityIterator = entityIterable.iterator();
        while ( entityIterator.hasNext() )
        {
            PropertyEntry<Relationship> entityPropertyEntry = entityIterator.next();

            String propertyKey = entityPropertyEntry.key();
            Relationship updatedEntity = entityPropertyEntry.entity();



            String entityUid = Long.toString( updatedEntity.getId() );

            // Check if the relationship has been deleted
            if ( StreamSupport.stream( tx.deletedRelationships().spliterator(), false ).map( n -> n.getId() ).collect( Collectors.toList() ).contains(
                    updatedEntity.getId() ) )
            {
                // If deleted get the old value of uidKey
                entityUid = uidFunction.apply( updatedEntity.getId() );
            }
            else
            {
                // If not deleted set the entityUid based which property was removed
                entityUid = (actionType.equals( ActionType.REMOVED ) && propertyKey.equals( uidKey ) ) ? uidFunction.apply( updatedEntity.getId() )
                                                                                                       : (String) updatedEntity.getProperty( uidKey, Long.toString( updatedEntity.getId() ) );
            }

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


    public static Map<String,List<Map<String,Object>>> assignedNodePropertyMapByUid( TransactionData tx, String uidKey )
    {
        return updatedNodePropertyMapByUid( tx, tx.assignedNodeProperties(), uidKey, ActionType.ADDED, ( l ) -> getNodeUidFromPreviousCommit( tx, uidKey, l ) );
    }

    public static Map<String,List<Map<String,Object>>> assignedRelationshipPropertyMapByUid( TransactionData tx, String uidKey )
    {
        return updatedRelationshipPropertyMapByUid( tx, tx.assignedRelationshipProperties(), uidKey, ActionType.ADDED,
                ( l ) -> getRelationshipUidFromPreviousCommit( tx, uidKey, l ) );
    }

    public static Map<String,List<Map<String,Object>>> removedNodePropertyMapByUid( TransactionData tx, String uidKey )
    {
        return updatedNodePropertyMapByUid( tx, tx.removedNodeProperties(), uidKey, ActionType.REMOVED, ( l ) -> getNodeUidFromPreviousCommit( tx, uidKey, l ) );
    }

    public static Map<String,List<Map<String,Object>>> removedRelationshipPropertyMapByUid( TransactionData tx, String uidKey )
    {
        return updatedRelationshipPropertyMapByUid( tx, tx.removedRelationshipProperties(), uidKey, ActionType.REMOVED,
                ( l ) -> getRelationshipUidFromPreviousCommit( tx, uidKey, l ) );
    }

    public static Map<String,List<String>> updatedRelationshipPropertyMapByKey( TransactionData tx,
            Iterable<PropertyEntry<Relationship>> entityIterable, String uidKey, ActionType actionType, Function<Long,String> uidFunction )
    {
        Map<String,List<String>> propertyChanges = new HashMap<>();

        Iterator<PropertyEntry<Relationship>> entityIterator = entityIterable.iterator();
        while ( entityIterator.hasNext() )
        {
            PropertyEntry<Relationship> entityPropertyEntry = entityIterator.next();

            String propertyKey = entityPropertyEntry.key();
            Relationship updatedEntity = entityPropertyEntry.entity();

            String entityUid = Long.toString( updatedEntity.getId() );

            // Check if the relationship has been deleted
            if ( StreamSupport.stream( tx.deletedRelationships().spliterator(), false ).map( n -> n.getId() ).collect( Collectors.toList() ).contains(
                    updatedEntity.getId() ) )
            {
                // If deleted get the old value of uidKey
                entityUid = getRelationshipUidFromPreviousCommit( tx, uidKey, updatedEntity.getId() );
            }
            else
            {
                // If not deleted set the entityUid based which property was removed
                entityUid = (actionType.equals( ActionType.REMOVED ) && propertyKey.equals( uidKey ) ) ? getRelationshipUidFromPreviousCommit( tx, uidKey, updatedEntity.getId() )
                                                                                                       : (String) updatedEntity.getProperty( uidKey, Long.toString( updatedEntity.getId() ) );
            }

            if ( !propertyChanges.containsKey( propertyKey ) )
            {
                propertyChanges.put( propertyKey, new ArrayList<>() );
            }

            propertyChanges.get( propertyKey ).add( entityUid );

        }

        return propertyChanges.isEmpty() ? Collections.emptyMap() : propertyChanges;
    }

    public static Map<String,List<String>> assignedRelationshipPropertyMapByKey( TransactionData tx, String uidKey )
    {
        return updatedRelationshipPropertyMapByKey( tx, tx.assignedRelationshipProperties(), uidKey, ActionType.ADDED,
                ( l ) -> getRelationshipUidFromPreviousCommit( tx, uidKey, l ) );
    }

    public static Map<String,List<String>> removedRelationshipPropertyMapByKey( TransactionData tx, String uidKey )
    {
        return updatedRelationshipPropertyMapByKey( tx, tx.removedRelationshipProperties(), uidKey, ActionType.REMOVED,
                ( l ) -> getRelationshipUidFromPreviousCommit( tx, uidKey, l ) );
    }

    public static Map<String,Map<String,List<Map<String,Object>>>> updatedRelationshipPropertyMapByType( TransactionData tx,
            Iterable<PropertyEntry<Relationship>> entityIterable, String uidKey, ActionType actionType, Function<Long,String> uidFunction )
    {
        Map<String,Map<String,List<Map<String,Object>>>> propertyChanges = new HashMap<>();

        Iterator<PropertyEntry<Relationship>> entityIterator = entityIterable.iterator();
        while ( entityIterator.hasNext() )
        {
            PropertyEntry<Relationship> entityPropertyEntry = entityIterator.next();

            String propertyKey = entityPropertyEntry.key();
            Relationship updatedEntity = entityPropertyEntry.entity();

            // Check if the property was removed and if it was, check if the property removed was the same property as the uidKey.
            String entityUid = Long.toString( updatedEntity.getId() );

            List<String> types = new ArrayList<>();

            if ( actionType.equals( ActionType.REMOVED ) )
            {
                // Get the nodes where the node properties were removed
                Relationship relationship = ((Function<Long,Relationship>) ( Long id ) -> StreamSupport.stream( tx.removedRelationshipProperties().spliterator(), true ).filter(
                        relationshipPropertyEntry -> id.equals( relationshipPropertyEntry.entity().getId() ) ).reduce( ( t1, t2 ) -> t1 ).get().entity()).apply(
                        updatedEntity.getId() );

                // Check if the relationship has been deleted
                if ( StreamSupport.stream( tx.deletedRelationships().spliterator(), false ).map( Relationship::getId ).collect( Collectors.toList() ).contains(
                        relationship.getId() ) )
                {
                    // If deleted get the type from the transaction data. (?)
                    types.addAll(  StreamSupport.stream( tx.deletedRelationships().spliterator(), false ).filter( rel -> rel.getId() == relationship.getId() ).map( rel -> rel.getType().name() ).collect( Collectors.toList()) );

                    // Also if deleted get the old value of uidKey
                    entityUid = uidFunction.apply( updatedEntity.getId() );
                }
                else
                {
                    types.add( relationship.getType().name() );

                    // If not deleted set the entityUid based which property was removed
                    entityUid = (actionType.equals( ActionType.REMOVED ) && propertyKey.equals( uidKey ) ) ? uidFunction.apply( updatedEntity.getId() )
                                                                                                           : (String) updatedEntity.getProperty( uidKey, Long.toString( updatedEntity.getId() ) );
                }

            }
            else
            {
                types.add( updatedEntity.getType().name() );
                entityUid = (String) updatedEntity.getProperty( uidKey, Long.toString( updatedEntity.getId() ) );
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

    public static Map<String,Map<String,List<Map<String,Object>>>> assignedRelationshipPropertyMapByType( TransactionData tx, String uidKey )
    {
        return updatedRelationshipPropertyMapByType( tx, tx.assignedRelationshipProperties(), uidKey, ActionType.ADDED,
                ( l ) -> getRelationshipUidFromPreviousCommit( tx, uidKey, l ) );
    }

    public static Map<String,Map<String,List<Map<String,Object>>>> removedRelationshipPropertyMapByType( TransactionData tx, String uidKey )
    {
        return updatedRelationshipPropertyMapByType( tx, tx.removedRelationshipProperties(), uidKey, ActionType.REMOVED,
                ( l ) -> getRelationshipUidFromPreviousCommit( tx, uidKey, l ) );
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

    public static String getNodeUidFromPreviousCommit( TransactionData tx, String uidKey, Long id )
    {
        String result = StreamSupport.stream( tx.removedNodeProperties().spliterator(), true )
                .filter( ( p ) -> p.key().equals( uidKey ) )
                .filter( ( p ) -> p.entity().getId() == id ).map( ( p ) -> (String) p.previouslyCommitedValue() )
                .collect( Collectors.joining() );

        return (result.isEmpty()) ? Long.toString( id ) : result;
    }

    ;

    public static String getRelationshipUidFromPreviousCommit( TransactionData tx, String uidKey, Long id )
    {
        String result = StreamSupport.stream( tx.removedRelationshipProperties().spliterator(), true )
                .filter( ( p ) -> p.key().equals( uidKey ) )
                .filter( ( p ) -> p.entity().getId() == id ).map( ( p ) -> (String) p.previouslyCommitedValue() )
                .collect( Collectors.joining() );

        return (result.isEmpty()) ? Long.toString( id ) : result;
    }

    ;
}
