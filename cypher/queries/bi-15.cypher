MATCH path=allShortestPaths((p1:Person {id: $person1Id})-[:KNOWS*]-(p2:Person {id: $person2Id}))
WITH [person IN nodes(path) | person.id] AS personIds, relationships(path) AS edges
// use 'KNOWS' edges both ways to cover both directions in cases 1 and 2
UNWIND [e IN edges | [startNode(e), endNode(e)]]
     + [e IN edges | [endNode(e), startNode(e)]]
    AS edge
WITH personIds, edge[0] AS pA, edge[1] AS pB, 0 AS edgeWeights

// case 1: every reply (by one of the Persons) to a Post (by the other Person) is worth 1.0 point
OPTIONAL MATCH
  (pA)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(pB),
  (post)<-[:CONTAINER_OF]-(forum:Forum)
WHERE forum.creationDate >= $startDate
  AND forum.creationDate <= $endDate
WITH personIds, pA, pB, edgeWeights + count(c)*1.0 AS edgeWeights

// case 2: every reply (by ones of the Persons) to a Comment (by the other Person) is worth 0.5 points
OPTIONAL MATCH
  (pA)<-[:HAS_CREATOR]-(c1:Comment)-[:REPLY_OF]->(c2:Comment)-[:HAS_CREATOR]->(pB),
  (c2)-[:REPLY_OF*]->(:Post)<-[:CONTAINER_OF]-(forum:Forum)
WHERE forum.creationDate >= $startDate
  AND forum.creationDate <= $endDate
WITH personIds, pA, pB, edgeWeights + count(c1)*0.5 AS edgeWeights

WITH personIds, sum(edgeWeights) AS weight

RETURN
  personIds,
  weight
ORDER BY
  weight DESC,
  personIds ASC
