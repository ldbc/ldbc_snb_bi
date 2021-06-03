----------------------------------------------------------------------------------------------------
-- DEL1 --------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
DELETE FROM Person
USING Person_Delete_candidates
WHERE Person_Delete_candidates.id = Person.id
;

DELETE FROM Person_likes_Comment
USING Person_Delete_candidates
WHERE Person_Delete_candidates.id = Person_likes_Comment.id
;

DELETE FROM Person_likes_Post
USING Person_Delete_candidates
WHERE Person_Delete_candidates.id = Person_likes_Post.id
;

DELETE FROM Person_workAt_Company
USING Person_Delete_candidates
WHERE Person_Delete_candidates.id = Person_workAt_Company.id
;

DELETE FROM Person_studyAt_University
USING Person_Delete_candidates
WHERE Person_Delete_candidates.id = Person_studyAt_University.id
;

-- treat KNOWS edges as undirected
DELETE FROM Person_knows_Person
USING Person_Delete_candidates
WHERE Person_Delete_candidates.id = Person_knows_Person.person1Id
   OR Person_Delete_candidates.id = Person_knows_Person.person2Id
;

DELETE FROM Person_hasInterest_Tag
USING Person_Delete_candidates
WHERE Person_Delete_candidates.id = Person_hasInterest_Tag.id
;

DELETE FROM Forum_hasMember_Person
USING Person_Delete_candidates
WHERE Person_Delete_candidates.id = Forum_hasMember_Person.id
;

UPDATE Forum
SET hasModerator_Person = NULL
WHERE Forum.title LIKE 'Group %'
  AND hasModerator_Person IN (SELECT id FROM Person_Delete_candidates)
;

-- offload cascading Forum deletes to DEL4
INSERT INTO Forum_Delete_candidates
SELECT Person_Delete_candidates.deletionDate AS deletionDate, Forum.id AS id
FROM Person_Delete_candidates
JOIN Forum
  ON Forum.hasModerator_Person = Person_Delete_candidates.id
WHERE Forum.title LIKE 'Album %'
   OR Forum.title LIKE 'Wall %'
;

-- offload cascading Post deletes to DEL6
INSERT INTO Post_Delete_candidates
SELECT Person_Delete_candidates.deletionDate AS deletionDate, Post.id AS id
FROM Person_Delete_candidates
JOIN Post
  ON Post.hasCreator_Person = Person_Delete_candidates.id
;

-- offload cascading Comment deletes to DEL7
INSERT INTO Comment_Delete_candidates
SELECT Person_Delete_candidates.deletionDate AS deletionDate, Comment.id AS id
FROM Person_Delete_candidates
JOIN Comment
  ON Comment.hasCreator_Person = Person_Delete_candidates.id
;

----------------------------------------------------------------------------------------------------
-- DEL2 --------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
DELETE FROM Person_likes_Post
USING Person_likes_Post_Delete_candidates
WHERE Person_likes_Post_Delete_candidates.src = Person_likes_Post.id
  AND Person_likes_Post_Delete_candidates.trg = Person_likes_Post.likes_post
;

----------------------------------------------------------------------------------------------------
-- DEL3 --------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
DELETE FROM Person_likes_Comment
USING Person_likes_Comment_Delete_candidates
WHERE Person_likes_Comment_Delete_candidates.src = Person_likes_Comment.id
  AND Person_likes_Comment_Delete_candidates.trg = Person_likes_Comment.likes_comment
;

----------------------------------------------------------------------------------------------------
-- DEL4 --------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
DELETE FROM Forum
USING Forum_Delete_candidates
;

DELETE FROM Forum_hasMember_Person
USING Forum_Delete_candidates
WHERE Forum_Delete_candidates.id = Forum_hasMember_Person.id
;

-- offload cascading Post deletes to DEL6
INSERT INTO Post_Delete_candidates
SELECT Forum_Delete_candidates.deletionDate AS deletionDate, Post.id AS id
FROM Post
JOIN Forum_Delete_candidates
  ON Forum_Delete_candidates.id = Post.forum_ContainerOf
;

----------------------------------------------------------------------------------------------------
-- DEL5 --------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
DELETE FROM Forum_hasMember_Person
USING Forum_hasMember_Person_Delete_candidates
WHERE Forum_hasMember_Person_Delete_candidates.src = Forum_hasMember_Person.id
  AND Forum_hasMember_Person_Delete_candidates.trg = Forum_hasMember_Person.hasMember_Person
;

----------------------------------------------------------------------------------------------------
-- DEL6 --------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
DELETE FROM Post
USING Post_Delete_candidates -- starting from the delete candidate post
WHERE Post_Delete_candidates.id = Post.id
;

DELETE FROM Person_likes_Post
USING Person_likes_Post_Delete_candidates
WHERE Person_likes_Post_Delete_candidates.trg = Person_likes_Post.likes_Post
;

DELETE FROM Post_hasTag_Tag
USING Post_Delete_candidates
WHERE Post_Delete_candidates.id = Post_hasTag_Tag.id
;

-- Offload cascading deletes to DEL7
INSERT INTO Comment_Delete_candidates 
SELECT Post_Delete_candidates.deletionDate AS deletionDate, Comment.id AS id
FROM Comment
JOIN Post_Delete_candidates
  ON Post_Delete_candidates.id = Comment.replyOf_Post
;

----------------------------------------------------------------------------------------------------
-- DEL7 --------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
DELETE FROM Comment
USING (
  WITH RECURSIVE message_thread AS (
      SELECT id
      FROM Comment_Delete_candidates -- starting from the delete candidate comments
      UNION
      SELECT Comment.id AS id
      FROM message_thread
      JOIN Comment
        ON Comment.replyof_comment = message_thread.id
        OR Comment.replyof_post = message_thread.id
  )
  SELECT id
  FROM message_thread
  ) sub
WHERE sub.id = Comment.id
;

DELETE FROM Person_likes_Comment
USING (
  WITH RECURSIVE message_thread AS (
      SELECT id
      FROM Comment_Delete_candidates -- starting from the delete candidate comments
      UNION
      SELECT Comment.id AS id
      FROM message_thread
      JOIN Comment
        ON Comment.replyof_comment = message_thread.id
        OR Comment.replyof_post = message_thread.id
  )
  SELECT id
  FROM message_thread
  ) sub
WHERE sub.id = Person_likes_Comment.likes_Comment
;

DELETE FROM Comment_hasTag_Tag
USING (
  WITH RECURSIVE message_thread AS (
      SELECT id
      FROM Comment_Delete_candidates -- starting from the delete candidate comments
      UNION ALL
      SELECT comment.id AS id
      FROM message_thread
      JOIN comment
        ON comment.replyof_comment = message_thread.id
        OR comment.replyof_post = message_thread.id
  )
  SELECT id
  FROM message_thread
  ) sub
WHERE sub.id = Comment_hasTag_Tag.id
;

----------------------------------------------------------------------------------------------------
-- DEL8 --------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
DELETE FROM Person_knows_Person
USING Person_knows_Person_Delete_candidates
WHERE (Person_knows_Person.Person1Id = Person_knows_Person_Delete_candidates.src AND Person_knows_Person.Person2Id = Person_knows_Person_Delete_candidates.trg)
   OR (Person_knows_Person.Person1Id = Person_knows_Person_Delete_candidates.trg AND Person_knows_Person.Person2Id = Person_knows_Person_Delete_candidates.src)
;
