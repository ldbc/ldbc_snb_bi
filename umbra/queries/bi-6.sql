/* Q6. Most authoritative users on a given topic
\set tag '\'Arnold_Schwarzenegger\''
 */
WITH poster_w_liker AS (
  SELECT DISTINCT
         m1.CreatorPersonId AS posterPersonid
       , l2.PersonId AS likerPersonid
    FROM Tag
    JOIN Message_hasTag_Tag
      ON Message_hasTag_Tag.TagId = Tag.id
       -- as an optimization, we use that the set of message1 is the same as message2
    JOIN Message m1
      ON m1.MessageId = Message_hasTag_Tag.MessageId
   LEFT JOIN Person_likes_Message l2
          ON m1.MessageId = l2.MessageId
       --, person p2 -- we don't need the person itself as its ID is in the like l2
   WHERE Tag.name = :tag
)
, popularity_score AS (
  SELECT m3.CreatorPersonId AS PersonId, count(*) AS popularityScore
    FROM Message m3
    JOIN Person_likes_Message l3
      ON l3.MessageId = m3.MessageId
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
