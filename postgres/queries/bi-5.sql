/* Q5. Most active posters in a given topic
\set tag '\'Abbas_I_of_Persia\''
 */
WITH detail AS (
SELECT CreatorPerson.id AS CreatorPersonId
     , count(DISTINCT Comment.Id)  AS replyCount
     , count(DISTINCT Person_likes_Message.MessageId||' '||Person_likes_Message.PersonId) AS likeCount
     , count(DISTINCT Message.Id)  AS messageCount
     , NULL as score
  FROM Tag
     , Message_hasTag_Tag
     , Message
  LEFT JOIN Comment
         ON Message.id = coalesce(Comment.ParentPostId, Comment.ParentCommentId)
  LEFT JOIN Person_likes_Message
         ON Message.id = Person_likes_Message.MessageId
     , Person CreatorPerson -- creator
 WHERE
    -- join
       Tag.id = Message_hasTag_Tag.TagId
   AND Message_hasTag_Tag.MessageId = Message.id
   AND Message.CreatorPersonId = CreatorPerson.id
    -- filter
   AND Tag.name = :tag
 GROUP BY CreatorPerson.id
)
SELECT CreatorPersonId AS "person.id"
     , replyCount
     , likeCount
     , messageCount
     , 1*messageCount + 2*replyCount + 10*likeCount AS score
  FROM detail
 ORDER BY score DESC, CreatorPersonId
 LIMIT 100
;
