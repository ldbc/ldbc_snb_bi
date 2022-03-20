/* Q7. Related topics
\set tag '\'Enrique_Iglesias\''
 */
SELECT RelatedTag.name AS "relatedTag.name"
     , count(*) AS count
  FROM Tag
  INNER JOIN Message_hasTag_Tag ParentMessage_HasTag_Tag
          ON Tag.id = ParentMessage_HasTag_Tag.TagId
  -- as an optimization, we don't need message here as it's ID is in ParentMessage_HasTag_Tag
  -- so proceed to the comment directly
  INNER JOIN Comment
          ON ParentMessage_HasTag_Tag.MessageId = coalesce(Comment.ParentCommentId, Comment.ParentPostId)
  -- comment's tag
  INNER JOIN Message_hasTag_Tag ct
          ON Comment.id = ct.MessageId
  INNER JOIN Tag RelatedTag
          ON ct.TagId = RelatedTag.id
  -- comment doesn't have the given tag: antijoin in the where clause
  LEFT  JOIN Message_hasTag_Tag nt
          ON Comment.id = nt.MessageId
         AND nt.TagId = ParentMessage_HasTag_Tag.TagId
 WHERE nt.MessageId IS NULL -- antijoin: comment (c) does not have the given tag
    -- filter
   AND Tag.name = :tag
 GROUP BY RelatedTag.name
 ORDER BY count DESC, RelatedTag.name
 LIMIT 100
;
