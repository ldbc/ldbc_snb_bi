// Q4. Top message creators in a country
/*
:param date => datetime('2010-01-29') AS date
*/
MATCH (country:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(person:Person)<-[:HAS_MEMBER]-(forum:Forum)
WHERE forum.creationDate > $date
WITH country, forum, count(person) AS numberOfMembers
ORDER BY numberOfMembers DESC, forum.id ASC, country.id
WITH DISTINCT forum AS topForum
LIMIT 100
SET topForum:TopForum

WITH count(*) AS dummy

MATCH
  (topForum2:TopForum)-[:HAS_MEMBER]->(person:Person)
OPTIONAL MATCH
  (person)<-[:HAS_CREATOR]-(message:Message)-[:REPLY_OF*0..]->(post:Post)<-[:CONTAINER_OF]-(topForum1:TopForum)
WITH person, message
WHERE message.creationDate > $date
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

WITH
  collect({
    personId: personId,
    personFirstName: personFirstName,
    personLastName: personLastName,
    personCreationDate: personCreationDate,
    messageCount: messageCount
  }) AS results

MATCH (topForum:TopForum)
REMOVE topForum:TopForum

WITH
  count(*) AS dummy,
  results

UNWIND results AS r
RETURN
  r.personId AS personId,
  r.personFirstName AS personFirstName,
  r.personLastName AS personLastName,
  r.personCreationDate AS personCreationDate,
  r.messageCount AS messageCount
