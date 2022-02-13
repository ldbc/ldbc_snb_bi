// Q12. How many persons have a given number of posts
/*
:param [{ startDate, lengthThreshold, languages }] => { RETURN
  datetime('2010-07-22') AS startDate,
  20 AS lengthThreshold,
  ['ar', 'hu'] AS languages
}
*/
MATCH (person:Person)
OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(message:Message)-[:REPLY_OF*0..]->(post:Post)
WHERE message.content IS NOT NULL
  AND message.length < $lengthThreshold
  AND message.creationDate > $startDate
  AND post.language IN $languages
WITH
  person,
  count(message) AS messageCount
RETURN
  messageCount,
  count(person) AS personCount
ORDER BY
  personCount DESC,
  messageCount DESC
