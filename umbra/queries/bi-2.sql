/* Q2. Tag evolution
\set date '\'2012-06-01\''::timestamp
\set tagClass '\'MusicalArtist\''
 */
WITH detail AS (
SELECT Tag.name AS TagName
     , count(DISTINCT CASE WHEN Message.creationDate <  :date + INTERVAL '100 days' THEN Message.MessageId ELSE NULL END) AS countMonth1
     , count(DISTINCT CASE WHEN Message.creationDate >= :date + INTERVAL '100 days' THEN Message.MessageId ELSE NULL END) AS countMonth2
  FROM Message
  JOIN Message_hasTag_Tag
    ON Message_hasTag_tag.MessageId = Message.MessageId
  JOIN Tag
    ON Tag.id = Message_hasTag_tag.TagId
  JOIN TagClass
    ON TagClass.id = Tag.TypeTagClassId
   AND TagClass.name = :tagClass
 WHERE :date <= Message.creationDate
   AND Message.creationDate <= :date + INTERVAL '200 days'
 GROUP BY Tag.name
)
SELECT TagName AS "tag.name"
     , countMonth1
     , countMonth2
     , abs(countMonth1-countMonth2) AS diff
  FROM detail
 ORDER BY diff desc, TagName
 LIMIT 100
;
