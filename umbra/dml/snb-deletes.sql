----------------------------------------------------------------------------------------------------
-- DEL1 --------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
DELETE FROM Person
USING Person_Delete_candidates
WHERE Person_Delete_candidates.id = Person.id
;

DELETE FROM Person_likes_Comment
USING Person_Delete_candidates
WHERE Person_Delete_candidates.id = Person_likes_Comment.PersonId
;

DELETE FROM Person_likes_Post
USING Person_Delete_candidates
WHERE Person_Delete_candidates.id = Person_likes_Post.PersonId
;

DELETE FROM Person_workAt_Company
USING Person_Delete_candidates
WHERE Person_Delete_candidates.id = Person_workAt_Company.PersonId
;

DELETE FROM Person_studyAt_University
USING Person_Delete_candidates
WHERE Person_Delete_candidates.id = Person_studyAt_University.PersonId
;

-- treat KNOWS edges as undirected
DELETE FROM Person_knows_Person
USING Person_Delete_candidates
WHERE Person_Delete_candidates.id = Person_knows_Person.Person1Id
   OR Person_Delete_candidates.id = Person_knows_Person.Person2Id
;

DELETE FROM Person_hasInterest_Tag
USING Person_Delete_candidates
WHERE Person_Delete_candidates.id = Person_hasInterest_Tag.PersonId
;

DELETE FROM Forum_hasMember_Person
USING Person_Delete_candidates
WHERE Person_Delete_candidates.id = Forum_hasMember_Person.PersonId
;

UPDATE Forum
SET ModeratorPersonId = NULL
WHERE Forum.title LIKE 'Group %'
  AND ModeratorPersonId IN (SELECT id FROM Person_Delete_candidates)
;

-- offload cascading Forum deletes to DEL4
INSERT INTO Forum_Delete_candidates
SELECT Person_Delete_candidates.deletionDate AS deletionDate, Forum.id AS id
FROM Person_Delete_candidates
JOIN Forum
  ON Forum.ModeratorPersonId = Person_Delete_candidates.id
WHERE Forum.title LIKE 'Album %'
   OR Forum.title LIKE 'Wall %'
;

-- offload cascading Post deletes to DEL6
INSERT INTO Post_Delete_candidates
SELECT Person_Delete_candidates.deletionDate AS deletionDate, Post.id AS id
FROM Person_Delete_candidates
JOIN Post
  ON Post.CreatorPersonId = Person_Delete_candidates.id
;

-- offload cascading Comment deletes to DEL7
INSERT INTO Comment_Delete_candidates
SELECT Person_Delete_candidates.deletionDate AS deletionDate, Comment.id AS id
FROM Person_Delete_candidates
JOIN Comment
  ON Comment.CreatorPersonId = Person_Delete_candidates.id
;

----------------------------------------------------------------------------------------------------
-- DEL2 --------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
DELETE FROM Person_likes_Post
USING Person_likes_Post_Delete_candidates
WHERE Person_likes_Post_Delete_candidates.src = Person_likes_Post.PersonId
  AND Person_likes_Post_Delete_candidates.trg = Person_likes_Post.PostId
;

----------------------------------------------------------------------------------------------------
-- DEL3 --------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
DELETE FROM Person_likes_Comment
USING Person_likes_Comment_Delete_candidates
WHERE Person_likes_Comment_Delete_candidates.src = Person_likes_Comment.PersonId
  AND Person_likes_Comment_Delete_candidates.trg = Person_likes_Comment.CommentId
;

----------------------------------------------------------------------------------------------------
-- DEL4 --------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
DELETE FROM Forum
USING Forum_Delete_candidates
;

DELETE FROM Forum_hasMember_Person
USING Forum_Delete_candidates
WHERE Forum_Delete_candidates.id = Forum_hasMember_Person.ForumId
;

-- offload cascading Post deletes to DEL6
INSERT INTO Post_Delete_candidates
SELECT Forum_Delete_candidates.deletionDate AS deletionDate, Post.id AS id
FROM Post
JOIN Forum_Delete_candidates
  ON Forum_Delete_candidates.id = Post.ContainerForumId
;

----------------------------------------------------------------------------------------------------
-- DEL5 --------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
DELETE FROM Forum_hasMember_Person
USING Forum_hasMember_Person_Delete_candidates
WHERE Forum_hasMember_Person_Delete_candidates.src = Forum_hasMember_Person.ForumId
  AND Forum_hasMember_Person_Delete_candidates.trg = Forum_hasMember_Person.PersonId
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
WHERE Person_likes_Post_Delete_candidates.trg = Person_likes_Post.PostId
;

DELETE FROM Post_hasTag_Tag
USING Post_Delete_candidates
WHERE Post_Delete_candidates.id = Post_hasTag_Tag.PostId
;

-- Offload cascading deletes to DEL7
INSERT INTO Comment_Delete_candidates 
SELECT Post_Delete_candidates.deletionDate AS deletionDate, Comment.id AS id
FROM Comment
JOIN Post_Delete_candidates
  ON Post_Delete_candidates.id = Comment.ParentPostId
;

----------------------------------------------------------------------------------------------------
-- DEL7 --------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
DELETE FROM Comment
USING (
  WITH RECURSIVE MessageThread AS (
      SELECT id
      FROM Comment_Delete_candidates -- starting from the delete candidate comments
      UNION
      SELECT Comment.id AS id
      FROM MessageThread
      JOIN Comment
        ON Comment.ParentCommentId = MessageThread.id
        OR Comment.ParentPostId = MessageThread.id
  )
  SELECT id
  FROM MessageThread
  ) sub
WHERE sub.id = Comment.id
;

DELETE FROM Person_likes_Comment
USING (
  WITH RECURSIVE MessageThread AS (
      SELECT id
      FROM Comment_Delete_candidates -- starting from the delete candidate comments
      UNION
      SELECT Comment.id AS id
      FROM MessageThread
      JOIN Comment
        ON Comment.ParentCommentId = MessageThread.id
        OR Comment.ParentPostId = MessageThread.id
  )
  SELECT id
  FROM MessageThread
  ) sub
WHERE sub.id = Person_likes_Comment.CommentId
;

DELETE FROM Comment_hasTag_Tag
USING (
  WITH RECURSIVE MessageThread AS (
      SELECT id
      FROM Comment_Delete_candidates -- starting from the delete candidate comments
      UNION ALL
      SELECT comment.id AS id
      FROM MessageThread
      JOIN comment
        ON comment.ParentCommentId = MessageThread.id
        OR comment.ParentPostId = MessageThread.id
  )
  SELECT id
  FROM MessageThread
  ) sub
WHERE sub.id = Comment_hasTag_Tag.CommentId
;

----------------------------------------------------------------------------------------------------
-- DEL8 --------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
DELETE FROM Person_knows_Person
USING Person_knows_Person_Delete_candidates
WHERE (Person_knows_Person.Person1Id = Person_knows_Person_Delete_candidates.src AND Person_knows_Person.Person2Id = Person_knows_Person_Delete_candidates.trg)
   OR (Person_knows_Person.Person1Id = Person_knows_Person_Delete_candidates.trg AND Person_knows_Person.Person2Id = Person_knows_Person_Delete_candidates.src)
;
