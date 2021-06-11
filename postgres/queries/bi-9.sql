/* Q9. Top thread initiators
\set startDate '\'2011-10-01T00:00:00.000+00:00\''::timestamp
\set endDate '\'2011-10-15T00:00:00.000+00:00\''::timestamp
 */
WITH RECURSIVE message_all(PostId
                      , ThreadCreatorPersonId
                      , MessageId
                      , creationDate
                      , MessageType
                       ) AS (
    SELECT id AS PostId
         , CreatorPersonId AS ThreadCreatorPersonId
         , id AS MessageId
         , creationDate
         , 'Post'
      FROM Post
     WHERE creationDate BETWEEN :startDate AND :endDate
  UNION ALL
    SELECT psa.PostId AS PostId
         , psa.ThreadCreatorPersonId AS ThreadCreatorPersonId
         , id AS messageId
         , Comment.creationDate
         , 'Comment'
      FROM Comment
         , message_all psa
     WHERE coalesce(Comment.ParentPostId, Comment.ParentCommentId) = psa.MessageId
        -- this is a performance optimization only
       AND Comment.creationDate BETWEEN :startDate AND :endDate
)
SELECT Person.id AS "person.id"
     , Person.firstName AS "person.firstName"
     , Person.lastName AS "person.lastName"
     , count(DISTINCT psa.PostId) AS threadCount
     -- if the thread initiator message does not count as a reply
     --, count(DISTINCT CASE WHEN psa.psa_messagetype = 'Comment' then psa.psa_messageid ELSE null END) AS messageCount
     , count(DISTINCT psa.MessageId) AS messageCount
  FROM Person
  LEFT JOIN message_all psa
    ON Person.id = psa.ThreadCreatorPersonId
   AND psa.creationDate BETWEEN :startDate AND :endDate
 GROUP BY Person.id, Person.firstName, Person.lastName
 ORDER BY messageCount DESC, Person.id
 LIMIT 100
;
