/* Q15. Trusted connection paths through forums created in a given timeframe
\set person1Id 21990232564808
\set person2Id 26388279076936
\set startDate '\'2010-11-01\''::timestamp
\set endDate '\'2010-12-01\''::timestamp
 */

WITH RECURSIVE ReplyScores(ThreadId
                          , OriginalMessageCreatorPersonId
                          , ReplyMessageCreatorPersonId
                          , ReplyMessageId
                          , Score
                           ) AS (
    SELECT Post.id AS ThreadId
         , Post.CreatorPersonId AS OriginalMessageCreatorPersonId
         , Comment.CreatorPersonId AS ReplyMessageCreatorPersonId
         , Comment.id AS ReplyMessageId
         , 1.0 AS Score
      FROM Forum
      JOIN Post
        ON Post.ContainerForumId = Forum.id
      JOIN Comment
        ON coalesce(Comment.ParentPostId, Comment.ParentCommentId) = Post.id
     WHERE Forum.creationDate BETWEEN :startDate AND :endDate
  UNION ALL
    SELECT r.ThreadId AS ThreadId
         , r.ReplyMessageCreatorPersonId AS OriginalMessageCreatorPersonId
         , Comment.CreatorPersonId AS ReplyMessageCreatorPersonId
         , Comment.id AS ReplyMessageId
         , 0.5 AS Score
      FROM ReplyScores r
      JOIN Comment
        ON coalesce(Comment.ParentPostId, Comment.ParentCommentId) = r.ReplyMessageId
)
   , Person_pairScores_directed AS (
    SELECT OriginalMessageCreatorPersonId AS OriginalMessageAuthorPersonId
         , ReplyMessageCreatorPersonId AS ReplyMessageCreatorPersonId
         , sum(Score) AS score
      FROM ReplyScores
     WHERE
        -- discard self replies from the score earned
           OriginalMessageCreatorPersonId != ReplyMessageCreatorPersonId
     GROUP BY OriginalMessageCreatorPersonId, ReplyMessageCreatorPersonId
)
   , Person_pairScores AS (
        -- note: this should already have both (A, B, score) and (B, A, score)
    SELECT coalesce(s1.OriginalMessageAuthorPersonId, s2.ReplyMessageCreatorPersonId) AS person1Id
         , coalesce(s1.ReplyMessageCreatorPersonId, s2.OriginalMessageAuthorPersonId) AS person2Id
         , coalesce(s1.score, 0.0) + coalesce(s2.score, 0.0) AS score
      FROM Person_pairScores_directed s1
           FULL JOIN Person_pairScores_directed s2
                  ON s1.OriginalMessageAuthorPersonId = s2.ReplyMessageCreatorPersonId 
                 AND s1.ReplyMessageCreatorPersonId = s2.OriginalMessageAuthorPersonId
)
   , wknows AS (
        -- weighted knows
    SELECT Person_knows_Person.person1Id
         , Person_knows_Person.person2Id
         , coalesce(score, 0.0) AS score
      FROM Person_knows_Person
           LEFT JOIN Person_pairScores pps
                  ON Person_knows_Person.person1Id = pps.person1Id
                 AND Person_knows_Person.person2Id = pps.person2Id
)
   , paths(startPerson
         , endPerson
         , path
         , weight
         , hopCount
         , person2Reached -- shows if person2 has been reached by any paths produced in the iteration that produced the path represented by this row
          ) AS (
    SELECT person1Id AS startPerson
         , person2Id AS endPerson
         , ARRAY[person1Id, person2Id]::bigint[] AS path
         , score AS weight
         , 1 AS hopCount
         , max(CASE WHEN person2Id = :person2Id THEN 1 ELSE 0 END) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS person2Reached
      FROM wknows
     WHERE person1Id = :person1Id
  UNION ALL
    SELECT p.startPerson AS startPerson
         , person2Id AS endPerson
         , array_append(path, person2Id) AS path
         , weight + score AS weight
         , hopCount + 1 AS hopCount
         , max(CASE WHEN person2Id = :person2Id THEN 1 ELSE 0 END) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS person2Reached
      FROM paths p
      JOIN wknows k
        ON p.endPerson = k.person1Id
     WHERE NOT ARRAY[k.person2Id] <@ p.path -- person2Id is not in the path yet
        -- stop condition
       AND p.person2Reached = 0
)
SELECT path, weight
  FROM paths
 WHERE endPerson = :person2Id
 ORDER BY weight DESC, path
;
