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

MATCH
  (forum:PopularForum)-[:HAS_MEMBER]->(person:Person)
OPTIONAL MATCH
  (person)<-[:HAS_CREATOR]-(message:Message)-[:REPLY_OF*0..]->(post:Post)<-[:CONTAINER_OF]-(popularForum:PopularForum)
WITH
  person.id AS personId,
  person.firstName AS personFirstName,
  person.lastName AS personLastName,
  person.creationDate AS personCreationDate,
  count(DISTINCT message) AS messageCount
ORDER BY
  messageCount DESC,
  person.id ASC
LIMIT 100

WITH collect({ personId: personId, personFirstName: personFirstName, personLastName: personLastName, personCreationDate: personCreationDate, messageCount: messageCount }) AS results

MATCH (forum:PopularForum)
REMOVE forum:PopularForum

WITH count(*) AS dummy, results

UNWIND results AS r
RETURN
  r.personId AS personId,
  r.personFirstName AS personFirstName,
  r.personLastName AS personLastName,
  r.personCreationDate AS personCreationDate,
  r.messageCount AS messageCount
