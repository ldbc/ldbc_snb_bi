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
CALL gds.shortestPath.dijkstra.stream('bi15', {
  sourceNode: person1,
  targetNode: person2,
  relationshipWeightProperty: 'weight'
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
RETURN totalCost
