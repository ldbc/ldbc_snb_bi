MATCH (personA:Person)-[knows:KNOWS]-(personB:Person)
OPTIONAL MATCH (personA)<-[:HAS_CREATOR]-(:Message)-[replyOf:REPLY_OF]-(:Message)-[:HAS_CREATOR]->(personB)
WITH knows, count(replyOf) AS numReplies
WITH knows, CASE WHEN numReplies <> 0 THEN 1/numReplies ELSE 2^63-1 END AS weight
SET knows.q19weight = weight
