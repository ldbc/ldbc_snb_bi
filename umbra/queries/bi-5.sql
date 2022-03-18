/* Q5. Most active posters in a given topic
\set tag '\'Abbas_I_of_Persia\''
 */
WITH detail AS (
SELECT CreatorPerson.id AS CreatorPersonId
     , count(DISTINCT Comment.Id)  AS replyCount
     , count(DISTINCT Person_likes_Message.MessageId||' '||Person_likes_Message.PersonId) AS likeCount
     , count(DISTINCT MessageThread.MessageId) AS messageCount
     , NULL as score
  FROM Tag
  JOIN Message_hasTag_Tag
    ON Message_hasTag_Tag.TagId = Tag.id
  JOIN MessageThread
    ON MessageThread.MessageId = Message_hasTag_Tag.MessageId
  LEFT JOIN Comment
         ON MessageThread.MessageId = coalesce(Comment.ParentPostId, Comment.ParentCommentId)
  LEFT JOIN Person_likes_Message
         ON MessageThread.MessageId = Person_likes_Message.MessageId
  JOIN Person CreatorPerson -- creator
    ON CreatorPerson.id = MessageThread.CreatorPersonId
 WHERE Tag.name = :tag
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
