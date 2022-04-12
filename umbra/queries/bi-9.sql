/* Q9. Top thread initiators
\set startDate '\'2011-10-01\''::timestamp
\set endDate '\'2011-10-15\''::timestamp
 */
SELECT Person.id AS "person.id"
     , Person.firstName AS "person.firstName"
     , Person.lastName AS "person.lastName"
     , count(DISTINCT Message.RootPostId) AS threadCount
     , count(DISTINCT Message.MessageId) AS messageCount
  FROM Person
  JOIN Post_View Post
    ON Person.id = Post.CreatorPersonId
   AND Post.creationDate BETWEEN :startDate AND :endDate
  JOIN Message
    ON Post.id = Message.RootPostId
   AND Message.creationDate BETWEEN :startDate AND :endDate
 GROUP BY Person.id, Person.firstName, Person.lastName
 ORDER BY messageCount DESC, Person.id
 LIMIT 100
;
