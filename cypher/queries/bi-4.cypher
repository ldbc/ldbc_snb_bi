// Q4. Top message creators in a country
/*
:param date => datetime('2012-09-01') AS date
*/
MATCH (country:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(person:Person)<-[:HAS_MEMBER]-(forum:Forum)
WHERE forum.creationDate > $date
WITH country, forum, count(person) AS numberOfMembers
ORDER BY numberOfMembers DESC, forum.id ASC, country.id
WITH DISTINCT forum
LIMIT 100
SET forum:PopularForum

WITH count(*) AS dummy
UNWIND [] AS x
RETURN NULL AS personId, NULL AS personFirstName, NULL AS personLastName, NULL AS personCreationDate, NULL AS messageCount

  UNION ALL

MATCH
  (forum:PopularForum)-[:HAS_MEMBER]->(person:Person)
OPTIONAL MATCH
  (person)<-[:HAS_CREATOR]-(message:Message)-[:REPLY_OF*0..]->(post:Post)<-[:CONTAINER_OF]-(popularForum:PopularForum)
RETURN
  person.id AS personId,
  person.firstName AS personFirstName,
  person.lastName AS personLastName,
  person.creationDate AS personCreationDate,
  count(DISTINCT message) AS messageCount
ORDER BY
  messageCount DESC,
  person.id ASC
LIMIT 100

  UNION ALL

MATCH (forum:PopularForum)
REMOVE forum:PopularForum

WITH count(*) AS dummy
UNWIND [] AS x
RETURN NULL AS personId, NULL AS personFirstName, NULL AS personLastName, NULL AS personCreationDate, NULL AS messageCount
