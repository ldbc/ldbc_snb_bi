DROP TABLE IF EXISTS Message;

CREATE TABLE Message as (SELECT p.id as id, NULL as ParentMessageId, p.CreatorPersonId
                         FROM Post p
                         UNION ALL
                         SELECT c.id                                    as id,
                                coalesce(ParentPostId, ParentCommentId) as ParentMessageId,
                                c.CreatorPersonid
                         FROM comment c);


EXPLAIN SELECT Person_knows_Person.Person1Id AS Person1Id,
                                     Person_knows_Person.Person2Id AS person2Id,
                                     1.0 / (count(Message1.id))    AS weight
                              FROM Person_knows_Person
                                       JOIN Message Message1
                                            ON Message1.CreatorPersonId = Person_knows_Person.Person1Id
                                       JOIN Message Message2
                                            ON Message2.CreatorPersonId = Person_knows_Person.Person2Id
                                                AND (Message1.id = Message2.ParentMessageId
                                                    OR Message2.id = Message1.ParentMessageId)
                              GROUP BY Person_knows_Person.Person1Id, Person_knows_Person.Person2Id;