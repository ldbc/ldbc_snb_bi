MATCH (personA:Person)-[knows:KNOWS]-(personB:Person)
MATCH (personA)<-[:HAS_CREATOR]-(:Message)-[replyOf:REPLY_OF]-(:Message)-[:HAS_CREATOR]->(personB)
WHERE personA.id < personB.id
WITH knows, count(replyOf) AS numReplies
WHERE numReplies <> 0
SET knows.q19weight = 1.0/numReplies
