// Q15. Weighted interaction paths
// Requires the Neo4j Graph Data Science library
/*
:param [{ person1Id, person2Id, startDate, endDate }] => { RETURN
    14 AS person1Id,
    16 AS person2Id,
    datetime('2010-11-01') AS startDate,
    datetime('2010-12-01') AS endDate
}
*/
MATCH (person1:Person {id: $person1Id}), (person2:Person {id: $person2Id})
CALL gds.shortestPath.dijkstra.stream({
  nodeQuery: 'MATCH (p:Person) RETURN id(p) AS id',
  relationshipQuery: '
    MATCH (pA:Person)-[knows:KNOWS]-(pB:Person)

    OPTIONAL MATCH (pA)<-[:HAS_CREATOR]-(m1:Message)-[r:REPLY_OF]-(m2:Message)-[:HAS_CREATOR]->(pB)
    OPTIONAL MATCH (m1)-[:REPLY_OF*0..]->(p1:Post)<-[:CONTAINER_OF]-(forum1:Forum)
             WHERE forum1.creationDate >= datetime({epochmillis: ' + $startDate.epochMillis + '})
               AND forum1.creationDate <= datetime({epochmillis: ' + $endDate.epochMillis   + '})
    OPTIONAL MATCH (m2)-[:REPLY_OF*0..]->(p2:Post)<-[:CONTAINER_OF]-(forum2:Forum)
             WHERE forum2.creationDate >= datetime({epochmillis: ' + $startDate.epochMillis + '})
               AND forum2.creationDate <= datetime({epochmillis: ' + $endDate.epochMillis   + '})
    WITH pA, pB, 0.0
      + sum(CASE forum1 IS NOT NULL WHEN true THEN 0.5 ELSE 0.0 END)
      + sum(CASE forum2 IS NOT NULL WHEN true THEN 0.5 ELSE 0.0 END)
      + sum(CASE m1 = p1            WHEN true THEN 0.5 ELSE 0.0 END)
      + sum(CASE m2 = p2            WHEN true THEN 0.5 ELSE 0.0 END)
      AS w

    RETURN
       id(pA) AS source,
       id(pB) AS target,
       1/(w+1) AS weight
    ',
  sourceNode: person1,
  targetNode: person2,
  relationshipWeightProperty: 'weight'
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
RETURN totalCost
