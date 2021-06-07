/* Q8. Central person for a tag
\set tag '\'Che_Guevara\''
\set date '\'2011-07-22T00:00:00.000+00:00\''::timestamp
 */
WITH Person_interested_in_Tag AS (
    SELECT Person.id AS PersonId
      FROM Person
         , Person_hasInterest_Tag
         , Tag
     WHERE
        -- join
           Person.id = Person_hasInterest_Tag.PersonId
       AND Person_hasInterest_Tag.TagId = Tag.id
        -- filter
       AND Tag.name = :tag
)
   , Person_Message_score AS (
    SELECT Person.id AS PersonId
         , count(*) AS message_score
      FROM Message
         , Person
         , Message_hasTag_Tag
         , Tag
     WHERE
        -- join
           Message.CreatorPersonId = Person.id
       AND Message.id = Message_hasTag_Tag.MessageId
       AND Message_hasTag_Tag.TagId = Tag.id
        -- filter
       AND Message.creationDate > :date
       AND Tag.name = :tag
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
     , Person_knows_Person
     , Person_score f -- the friend
 WHERE
    -- join
       p.PersonId = Person_knows_Person.Person1Id
   AND Person_knows_Person.Person2Id = f.PersonId
 GROUP BY p.PersonId, p.score
 ORDER BY p.score + sum(f.score) DESC, p.PersonId
 LIMIT 100
;
