CALL gds.graph.create.cypher(
    'bi19',
    'MATCH (p:Person) RETURN id(p) AS id',
    'MATCH
      (personA:Person)-[:KNOWS]-(personB:Person),
      (personA)<-[:HAS_CREATOR]-(:Message)-[replyOf:REPLY_OF]-(:Message)-[:HAS_CREATOR]->(personB)
    RETURN
      id(personA) AS source,
      id(personB) AS target,
      1.0/count(replyOf) AS weight'
)
