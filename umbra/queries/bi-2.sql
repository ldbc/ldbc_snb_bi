/* Q2. Tag evolution
\set date '\'2012-06-01\''::timestamp
\set tagClass '\'MusicalArtist\''
 */
WITH detail AS (
SELECT Tag.name AS TagName
     , count(DISTINCT CASE WHEN Message.creationDate <  :date + INTERVAL '100 days' THEN Message.MessageId ELSE NULL END) AS countMonth1
     , count(DISTINCT CASE WHEN Message.creationDate >= :date + INTERVAL '100 days' THEN Message.MessageId ELSE NULL END) AS countMonth2
  FROM TagClass
  JOIN Tag
    ON Tag.TypeTagClassId = TagClass.id
  LEFT JOIN Message_hasTag_Tag
         ON Message_hasTag_tag.TagId = Tag.id
  LEFT JOIN Message
    ON Message.MessageId = Message_hasTag_tag.MessageId
   AND Message.creationDate >= :date
   AND Message.creationDate <  :date + INTERVAL '200 days'
 WHERE TagClass.name = :tagClass
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
