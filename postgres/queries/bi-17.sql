/* Q17. Information propagation analysis
\set tag '\'Frank_Sinatra\''
\set delta '4'
 */
SELECT Message1.CreatorPersonId AS "person1.id", count(message2) AS messageCount
FROM Tag
JOIN Message_hasTag_Tag Message1_hasTag_Tag
  ON Message1_hasTag_Tag.TagId = Tag.id
JOIN Message Message1
  ON Message1.id = Message1_hasTag_Tag.MessageId
JOIN MessageThread MessageThread1
  ON MessageThread1.MessageId = Message1.id
JOIN Post Post1
  ON Post1.id = MessageThread1.RootPostId
JOIN Message_hasTag_Tag Message2_hasTag_Tag
  ON Message2_hasTag_Tag.TagId = Tag.id
JOIN Message Message2
  ON Message2.id = Message2_hasTag_Tag.MessageId
JOIN Comment_hasTag_Tag
  ON Comment_hasTag_Tag.TagId = Tag.id
 AND Message1.creationDate + :delta * INTERVAL '1 day' < Message2.creationDate
JOIN Comment
  ON Comment.id = Comment_hasTag_Tag.CommentId
 AND coalesce(Comment.ParentPostId, Comment.ParentCommentId) = Message2.id
JOIN MessageThread MessageThread2
  ON MessageThread2.MessageId = Message2.id
JOIN Post Post2
  ON Post2.id = MessageThread2.RootPostId
 AND Post2.ContainerForumId != Post1.ContainerForumId -- forum2 != forum1
LEFT JOIN Forum_hasMember_Person Forum_hasMember_Person1
  ON Forum_hasMember_Person1.ForumId = Post2.ContainerForumId
 AND Forum_hasMember_Person1.PersonId = Message1.CreatorPersonId -- person1.id
JOIN Forum_hasMember_Person Forum_hasMember_Person2
  ON Forum_hasMember_Person2.PersonId = Comment.CreatorPersonId
JOIN Forum_hasMember_Person Forum_hasMember_Person3
  ON Forum_hasMember_Person3.PersonId = Message2.CreatorPersonId
WHERE Tag.name = :tag
GROUP BY Message1.CreatorPersonId
ORDER BY Message1.CreatorPersonId
;
