CALL gds.graph.create.cypher(
    'bi19',
    'MATCH (p:Person) RETURN id(p) AS id',
    'MATCH
      (personA:Person)-[:KNOWS]-(personB:Person),
      (personA)<-[:HAS_CREATOR]-(:Message)-[replyOf:REPLY_OF]-(:Message)-[:HAS_CREATOR]->(personB)
    WITH
      id(personA) AS source,
      id(personB) AS target,
      count(replyOf) AS numInteractions
    RETURN
      source,
      target,
      CASE WHEN floor(40-sqrt(numInteractions)) > 1 THEN floor(40-sqrt(numInteractions)) ELSE 1 END AS weight'
)
