/* Q2. Tag evolution
\set date '\'2012-06-01T00:00:00.000+00:00\''::timestamp
\set tagClass '\'MusicalArtist\''
 */
WITH detail AS (
SELECT Tag.name AS TagName
     , count(DISTINCT CASE WHEN Message.creationDate <  :date + INTERVAL '100 days' THEN Message.id ELSE NULL END) AS countMonth1
     , count(DISTINCT CASE WHEN Message.creationDate >= :date + INTERVAL '100 days' THEN Message.id ELSE NULL END) AS countMonth2
  FROM Message
     , Message_hasTag_Tag
     , Tag
     , TagClass
 WHERE
    -- join
       TagClass.id = Tag.TypeTagClassId
   AND Message.id = Message_hasTag_tag.MessageId
   AND Message_hasTag_tag.TagId = Tag.id
    -- filter
   AND TagClass.name = :tagClass
   AND :date <= Message.creationDate
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
