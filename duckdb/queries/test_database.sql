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


INSERT INTO results (SELECT p.id                                                                           as Person1id,
                            p2.id                                                                          as Person2id,
                            c.name                                                                         as Company,
                            cheapest_path(0, (select count(*) from PersonUniversity p), p.rowid, p2.rowid) as weight
                     FROM PersonUniversity p
                              JOIN Person_workAt_Company pwc on p.id = pwc.PersonId
                              JOIN Company c on (pwc.CompanyId = c.id AND c.name = 'British_NorthWest_Airlines')
                              JOIN PersonUniversity p2 on p2.id = 30786325592518
                     order by weight, p.id);


-- -- PARAMS
-- -- INSERT INTO results (SELECT p.id                                                                           as Person1id,
-- --                             p2.id                                                                          as Person2id,
-- --                             c.name                                                                         as Company,
-- --                             cheapest_path(0, (select count(*) from PersonUniversity p), p.rowid, p2.rowid) as weight
-- --                      FROM PersonUniversity p
-- --                               JOIN Person_workAt_Company pwc on p.id = pwc.PersonId
-- --                               JOIN Company c on (pwc.CompanyId = c.id AND c.name = ':company')
-- --                               JOIN PersonUniversity p2 on p2.id = :person2id
-- --                      where weight is not null
-- --                      order by weight, p.id);