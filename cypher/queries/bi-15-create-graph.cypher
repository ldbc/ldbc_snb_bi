CALL gds.graph.create.cypher(
    'bi15',
    'MATCH (p:Person) RETURN id(p) AS id',
    'MATCH (pA:Person)-[knows:KNOWS]-(pB:Person)

    // case 1: every reply (by one of the Persons) to a Post (by the other Person) is worth 1.0 point
    OPTIONAL MATCH
      (pA)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(post:Post)-[:HAS_CREATOR]->(pB),
      (post)<-[:CONTAINER_OF]-(forum:Forum)
    WHERE forum.creationDate >= datetime({epochmillis: ' + $startDate.epochMillis + '})
      AND forum.creationDate <= datetime({epochmillis: ' + $endDate.epochMillis   + '})
    WITH knows, pA, pB, count(c)*1.0 AS w1

    // case 2: every reply (by ones of the Persons) to a Comment (by the other Person) is worth 0.5 points
    OPTIONAL MATCH
      (pA)<-[:HAS_CREATOR]-(c1:Comment)-[:REPLY_OF]->(c2:Comment)-[:HAS_CREATOR]->(pB),
      (c2)-[:REPLY_OF*]->(:Post)<-[:CONTAINER_OF]-(forum:Forum)
    WHERE forum.creationDate >= datetime({epochmillis: ' + $startDate.epochMillis + '})
      AND forum.creationDate <= datetime({epochmillis: ' + $endDate.epochMillis   + '})
    WITH knows, pA, pB, w1+count(c1)*0.5 AS w2

    // case 1 reverse
    OPTIONAL MATCH
      (pA)<-[:HAS_CREATOR]-(post:Post)<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(pB),
      (post)<-[:CONTAINER_OF]-(forum:Forum)
    WHERE forum.creationDate >= datetime({epochmillis: ' + $startDate.epochMillis + '})
      AND forum.creationDate <= datetime({epochmillis: ' + $endDate.epochMillis   + '})
    WITH knows, pA, pB, w2+count(c)*1.0 AS w3

    // case 2 reverse
    OPTIONAL MATCH
      (pA)<-[:HAS_CREATOR]-(c2:Comment)<-[:REPLY_OF]->(c1:Comment)-[:HAS_CREATOR]->(pB),
      (c2)-[:REPLY_OF*]->(:Post)<-[:CONTAINER_OF]-(forum:Forum)
    WHERE forum.creationDate >= datetime({epochmillis: ' + $startDate.epochMillis + '})
      AND forum.creationDate <= datetime({epochmillis: ' + $endDate.epochMillis   + '})
    WITH knows, pA, pB, w3+count(c1)*0.5 AS w4

    RETURN
       id(pA) AS source,
       id(pB) AS target,
       1/(w4+1) AS weight'
)
