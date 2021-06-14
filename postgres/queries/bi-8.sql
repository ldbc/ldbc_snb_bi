/* Q8. Central person for a tag
\set tag '\'Che_Guevara\''
\set date '\'2011-07-22\''::timestamp
 */
WITH Person_interested_in_Tag AS (
    SELECT Person.id AS PersonId
      FROM Person
      JOIN Person_hasInterest_Tag
        ON Person_hasInterest_Tag.PersonId = Person.id
      JOIN Tag
        ON Tag.id = Person_hasInterest_Tag.TagId
       AND Tag.name = :tag
)
   , Person_Message_score AS (
    SELECT Person.id AS PersonId
         , count(*) AS message_score
      FROM Message
      JOIN Person
        ON Person.id = Message.CreatorPersonId
      JOIN Message_hasTag_Tag
        ON Message_hasTag_Tag.MessageId = Message.id
      JOIN Tag
        ON Tag.id = Message_hasTag_Tag.TagId
       AND Tag.name = :tag
     WHERE Message.creationDate > :date
     GROUP BY Person.id
)
   , Person_score AS (
    SELECT coalesce(Person_interested_in_Tag.PersonId, pms.PersonId) AS PersonId
         , CASE WHEN Person_interested_in_Tag.PersonId IS NULL then 0 ELSE 100 END -- scored from interest in the given tag
         + coalesce(pms.message_score, 0) AS score
      FROM Person_interested_in_Tag
           FULL JOIN Person_Message_score pms
                  ON Person_interested_in_Tag.PersonId = pms.PersonId
)
SELECT p.PersonId AS "person.id"
     , p.score AS score
     , sum(f.score) AS friendsScore
  FROM Person_score p
  JOIN Person_knows_Person
    ON Person_knows_Person.Person1Id = p.PersonId
  JOIN Person_score f -- the friend
    ON f.PersonId = Person_knows_Person.Person2Id
 GROUP BY p.PersonId, p.score
 ORDER BY p.score + sum(f.score) DESC, p.PersonId
 LIMIT 100
;
