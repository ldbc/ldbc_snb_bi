DROP TABLE IF EXISTS Message;
DROP TABLE IF EXISTS Interactions;
DROP TABLE IF EXISTS Person_InteractionsKnows_Person;

CREATE TABLE Message as (
    SELECT p.id as id, NULL as ParentMessageId, p.CreatorPersonId
    FROM Post p
    UNION ALL
    SELECT c.id as id, coalesce(ParentPostId, ParentCommentId) as ParentMessageId, c.CreatorPersonid
    FROM comment c
);

-- DEBUG
CREATE TABLE interactions as (
SELECT
        Person_knows_Person.Person1Id AS Person1Id,
        Person_knows_Person.Person2Id AS person2Id,
        1.0 / (count(Message1.id)) AS weight
    FROM Person_knows_Person
    JOIN Message Message1
      ON Message1.CreatorPersonId = Person_knows_Person.Person1Id
    JOIN Message Message2
      ON Message2.CreatorPersonId = Person_knows_Person.Person2Id
     AND (Message1.id = Message2.ParentMessageId
      OR  Message2.id = Message1.ParentMessageId)
    GROUP BY Person_knows_Person.Person1Id, Person_knows_Person.Person2Id);


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


