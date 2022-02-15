MATCH path=allShortestPaths((p1:Person {id: $person1Id})-[:KNOWS*]-(p2:Person {id: $person2Id}))
WITH relationships(path) AS edges
// use 'KNOWS' edges both ways to cover both directions in cases 1 and 2
UNWIND [e IN edges | [e, startNode(e), endNode(e)]]
     + [e IN edges | [e, endNode(e), startNode(e)]]
    AS edge
WITH DISTINCT edge[0] AS knows, edge[1] AS pA, edge[2] AS pB
SET knows.weight = 0
WITH knows, pA, pB

// case 1: every reply (by one of the Persons) to a Post (by the other Person) is worth 1.0 point
OPTIONAL MATCH
  (pA)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(pB),
  (post)<-[:CONTAINER_OF]-(forum:Forum)
WHERE forum.creationDate >= $startDate
  AND forum.creationDate <= $endDate
WITH knows, pA, pB, count(c)*1.0 AS w1

// case 2: every reply (by ones of the Persons) to a Comment (by the other Person) is worth 0.5 points
OPTIONAL MATCH
  (pA)<-[:HAS_CREATOR]-(c1:Comment)-[:REPLY_OF]->(c2:Comment)-[:HAS_CREATOR]->(pB),
  (c2)-[:REPLY_OF*]->(:Post)<-[:CONTAINER_OF]-(forum:Forum)
WHERE forum.creationDate >= $startDate
  AND forum.creationDate <= $endDate
WITH knows, pA, pB, w1+count(c1)*0.5 AS w2
SET knows.weight = knows.weight + w2

WITH count(*) AS dummy
MATCH path=allShortestPaths((p1:Person {id: $person1Id})-[:KNOWS*]-(p2:Person {id: $person2Id}))
RETURN [person IN nodes(path) | person.id] AS personIds, reduce(acc = 0, e IN relationships(path) | acc + e.weight) AS weight
ORDER BY
  weight DESC,
  personIds ASC
