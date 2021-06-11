/* Q9. Top thread initiators
\set startDate '\'2011-10-01\''::timestamp
\set endDate '\'2011-10-15\''::timestamp
 */
SELECT Person.id AS "person.id"
     , Person.firstName AS "person.firstName"
     , Person.lastName AS "person.lastName"
     , count(DISTINCT MessageThread.RootPostId) AS threadCount
     , count(DISTINCT MessageThread.MessageId) AS messageCount
  FROM Person
  LEFT JOIN Post
    ON Person.id = Post.CreatorPersonId
   AND Post.creationDate BETWEEN :startDate AND :endDate
  LEFT JOIN MessageThread
    ON Post.id = MessageThread.RootPostId
   AND MessageThread.creationDate BETWEEN :startDate AND :endDate
 GROUP BY Person.id, Person.firstName, Person.lastName
 ORDER BY messageCount DESC, Person.id
 LIMIT 100
;
