/* Q6. Most authoritative users on a given topic
\set tag '\'Arnold_Schwarzenegger\''
 */
WITH poster_w_liker AS (
  SELECT DISTINCT
         m1.CreatorPersonId AS posterPersonid
       , l2.PersonId AS likerPersonid
    FROM Tag
       , Message_hasTag_Tag
       -- as an optimization, we use that the set of message1 is the same as message2
       , Message m1
   LEFT JOIN Person_likes_Message l2
          ON m1.id = l2.MessageId
       --, person p2 -- we don't need the person itself as its ID is in the like l2
   WHERE
      -- join
         Tag.id = Message_hasTag_Tag.TagId
     AND Message_hasTag_Tag.MessageId = m1.id
      -- filter
     AND Tag.name = :tag
)
, popularity_score AS (
  SELECT m3.CreatorPersonId AS PersonId, count(*) AS popularityScore
    FROM Message m3
       , Person_likes_Message l3
   WHERE
      -- join
         m3.id = l3.MessageId
   GROUP BY m3.CreatorPersonId
)
SELECT pl.posterPersonid AS "person1.id"
     , sum(coalesce(ps.popularityScore, 0)) AS authorityScore
  FROM poster_w_liker pl
  LEFT JOIN popularity_score ps
         ON pl.likerPersonid = ps.PersonId
 GROUP BY pl.posterPersonid
 ORDER BY authorityScore DESC, pl.posterPersonid ASC
 LIMIT 100
;
