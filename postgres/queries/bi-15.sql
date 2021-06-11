/* Q15. Trusted connection paths through forums created in a given timeframe
\set person1id 21990232564808
\set person2id 26388279076936
\set startDate '\'2010-11-01T00:00:00.000+00:00\''::timestamp
\set endDate '\'2010-12-01T00:00:00.000+00:00\''::timestamp
 */

WITH RECURSIVE reply_scores(r_threadid
                          , r_orig_personid
                          , r_reply_personid
                          , r_reply_messageid
                          , r_score
                           ) AS (
    SELECT Post.id AS r_threadid
         , Post.CreatorPersonId AS r_orig_personid
         , Comment.CreatorPersonId AS r_reply_personid
         , Comment.id AS r_reply_messageid
         , 1.0 AS r_score
      FROM Forum
         , Post
         , Comment
     WHERE
        -- join
           Forum.id = Post.ContainerForumId
       AND Post.id = coalesce(Comment.ParentPostId, Comment.ParentCommentId)
        -- filter
       AND Forum.creationDate BETWEEN :startDate AND :endDate
  UNION ALL
    SELECT r.r_threadid AS r_threadid
         , r.r_reply_personid AS r_orig_personid
         , Comment.CreatorPersonId AS r_reply_personid
         , Comment.id AS r_reply_messageid
         , 0.5 AS r_score
      FROM reply_scores r
         , Comment
     WHERE
        -- join
           r.r_reply_messageid = coalesce(Comment.ParentPostId, Comment.ParentCommentId)
)
   , person_pair_scores_directed AS (
    SELECT r_orig_personid AS orig_personid
         , r_reply_personid AS reply_personid
         , sum(r_score) AS score
      FROM reply_scores
     WHERE
        -- discard self replies from the score earned
           r_orig_personid != r_reply_personid
     GROUP BY r_orig_personid, r_reply_personid
)
   , person_pair_scores AS (
        -- note: this should already have both (A, B, score) and (B, A, score)
    SELECT coalesce(s1.orig_personid, s2.reply_personid) AS Person1Id
         , coalesce(s1.reply_personid, s2.orig_personid) AS Person2Id
         , coalesce(s1.score, 0.0) + coalesce(s2.score, 0.0) AS score
      FROM person_pair_scores_directed s1
           FULL JOIN person_pair_scores_directed s2
                  ON s1.orig_personid = s2.reply_personid 
                 AND s1.reply_personid = s2.orig_personid
)
   , wknows AS (
        -- weighted knows
    SELECT Person_knows_Person.Person1Id
         , Person_knows_Person.Person2Id
         , coalesce(score, 0.0) AS score
      FROM Person_knows_Person
           LEFT JOIN person_pair_scores pps
                  ON Person_knows_Person.Person1Id = pps.Person1Id
                 AND Person_knows_Person.Person2Id = pps.Person2Id
)
   , paths(startPerson
         , endPerson
         , path
         , weight
         , hopCount
         , person2Reached -- shows if person2 has been reached by any paths produced in the iteration that produced the path represented by this row
          ) AS (
    SELECT Person1Id AS startPerson
         , Person2Id AS endPerson
         , ARRAY[Person1Id, Person2Id]::bigint[] AS path
         , score AS weight
         , 1 AS hopCount
         , max(CASE WHEN Person2Id = :person2id THEN 1 ELSE 0 END) OVER () AS person2Reached
      FROM wknows
     WHERE Person1Id = :person1id
  UNION ALL
    SELECT p.startPerson AS startPerson
         , Person2Id AS endPerson
         , array_append(path, Person2Id) AS path
         , weight + score AS weight
         , hopCount + 1 AS hopCount
         , max(CASE WHEN Person2Id = :person2id THEN 1 ELSE 0 END) OVER () AS person2Reached
      FROM paths p
         , wknows k
     WHERE
        -- join
           p.endPerson = k.Person1Id
       AND NOT p.path && ARRAY[k.Person2Id] -- Person2Id is not in the path
        -- stop condition
       AND p.person2Reached = 0
)
SELECT path, weight
  FROM paths
 WHERE endPerson = :person2id
 ORDER BY weight DESC, path
;
