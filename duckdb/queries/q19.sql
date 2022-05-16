DROP TABLE IF EXISTS Message;
DROP TABLE IF EXISTS Interactions;
DROP TABLE IF EXISTS PersonInteractions;

CREATE TABLE Message as (SELECT p.id as id, NULL as ParentMessageId, p.CreatorPersonId
                         FROM Post p
                         UNION ALL
                         SELECT c.id                                    as id,
                                coalesce(ParentPostId, ParentCommentId) as ParentMessageId,
                                c.CreatorPersonid
                         FROM comment c);

CREATE TABLE interactions as (SELECT Person_knows_Person.Person1Id AS Person1Id,
                                     Person_knows_Person.Person2Id AS person2Id,
                                     1.0 / (count(Message1.id))    AS weight
                              FROM Person_knows_Person
                                       JOIN Message Message1
                                            ON Message1.CreatorPersonId = Person_knows_Person.Person1Id
                                       JOIN Message Message2
                                            ON Message2.CreatorPersonId = Person_knows_Person.Person2Id
                                                AND (Message1.id = Message2.ParentMessageId
                                                    OR Message2.id = Message1.ParentMessageId)
                              GROUP BY Person_knows_Person.Person1Id, Person_knows_Person.Person2Id);


CREATE TABLE PersonInteractions AS (SELECT DISTINCT Person1id as id
                                    FROM ((SELECT Person1id
                                           FROM interactions)
                                          UNION ALL
                                          (SELECT Person2id AS Person1id
                                           FROM interactions))
                                    ORDER BY id);


SELECT DISTINCT CREATE_CSR(
               0,
               v.vcount,
               sub.dense_id,
               sub.cnt,
               r.src,
               r.dst,
               r.weight
           ) AS numEdges
FROM (SELECT p.rowid as dense_id, count(k.Person1id) as cnt
      FROM PersonInteractions p
               LEFT JOIN interactions k ON k.Person1id = p.id
      GROUP BY p.rowid) sub,
     (SELECT count(p.id) as vcount FROM PersonInteractions p) v,
     (SELECT src.rowid as src, dst.rowid as dst, t.weight as weight
      FROM interactions t
               JOIN PersonInteractions src ON t.Person1id = src.id
               JOIN PersonInteractions dst ON t.Person2id = dst.id) r
WHERE r.src = sub.dense_id;


-- SELECT min(CREATE_CSR_EDGE(0, (SELECT count(p.id) as vcount FROM PersonInteractions p),
--                            CAST((SELECT sum(CREATE_CSR_VERTEX(0,
--                                                               (SELECT count(p.id) as vcount FROM PersonInteractions p),
--                                                               sub.dense_id, sub.cnt)) AS numEdges
--                                  FROM (SELECT p.rowid as dense_id, count(k.Person1id) as cnt
--                                        FROM PersonInteractions p
--                                                 LEFT JOIN interactions k ON k.Person1id = p.id
--                                        GROUP BY p.rowid) sub) AS BIGINT),
--                            src.rowid, dst.rowid, k.weight))
-- FROM interactions k
--          JOIN PersonInteractions src ON k.Person1id = src.id
--          JOIN PersonInteractions dst ON k.Person2id = dst.id;


-- CREATE TABLE interactions as (
-- SELECT c.CreatorPersonId as Person1id, m.CreatorPersonId as Person2id, count(*) as interactions
-- FROM comment c
-- JOIN Message m on m.messageid = coalesce(c.parentpostid, c.parentcommentid)
-- where c.CreatorPersonId <> m.CreatorPersonId
-- group by c.CreatorPersonId, m.CreatorPersonid
-- );
--
-- INSERT INTO interactions (SELECT Person2id, Person1id, interactions from interactions);
--
-- -- DEBUG
-- SELECT person1id, person2id, sum(interactions) as interactions
-- FROM interactions
-- GROUP BY person1id, person2id;

-- DEBUG
-- CREATE TABLE Person_InteractionsKnows_Person AS (
--         SELECT pkp.person1id, pkp.person2id, 1.0 / cast((i.interactions + i2.interactions) as float) as weight
--         FROM person_knows_person pkp
--         JOIN interactions i on pkp.person1id = i.person1id and pkp.person2id = i.Person2id
--         JOIN interactions i2 on pkp.person1id = i2.person2id and pkp.person2id = i2.Person1id
--         JOIN person p on pkp.person1id = p.id
--         JOIN person p2 on pkp.person2id = p2.id
--         WHERE p.LocationCityId <> p2.LocationCityId
-- );


