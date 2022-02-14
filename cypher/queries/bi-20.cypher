// Q20. Recruitment
// Requires the Neo4j Graph Data Science library
/*
:param [{ company, person2Id }] => {
  RETURN
    'Falcon_Air' AS company,
    66 AS person2Id
  }
*/
MATCH
  (company:Company {name: $company})<-[:WORK_AT]-(person1:Person),
  (person2:Person {id: $person2Id})
CALL gds.shortestPath.dijkstra.stream('bi20', {
  sourceNode: person1,
  targetNode: person2,
  relationshipWeightProperty: 'weight'
})
YIELD totalCost
WHERE person1.id <> $person2Id
RETURN person1.id, totalCost AS totalWeight
ORDER BY totalWeight ASC, person1.id ASC
LIMIT 20
