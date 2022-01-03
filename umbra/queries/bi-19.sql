/* Q19. Interaction path between cities
\set city1Id '669'
\set city2Id '648'
 */
WITH RECURSIVE KnowsWeight AS ( -- computes weight based on the interaction of Person1 and Person2
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
    GROUP BY Person_knows_Person.Person1Id, Person_knows_Person.Person2Id
  ),
  Person2s AS (
    SELECT id
      FROM Person
     WHERE LocationCityId = :city2Id
  ),
  paths(startPerson
      , endPerson
      , path
      , weight
      , person2Reached -- shows if person2 has been reached by any paths produced in the iteration that produced the path represented by this row
      ) AS (
    SELECT Person1Id AS startPerson
         , Person2Id AS endPerson
         , ARRAY[Person1Id, Person2Id]::bigint[] AS path
         , weight
         , max(CASE WHEN Person2Id IN (SELECT id FROM Person2s) THEN 1 ELSE 0 END) OVER () AS person2Reached
      FROM KnowsWeight
     --WHERE Person1Id = :person1id
    UNION ALL
    SELECT paths.startPerson AS startPerson
         , KnowsWeight.Person2Id AS endPerson
         , array_append(path, Person2Id) AS path
         , KnowsWeight.weight + paths.weight AS weight
         , max(CASE WHEN Person2Id IN (SELECT id FROM Person2s) THEN 1 ELSE 0 END) OVER () AS person2Reached
      FROM paths
      JOIN KnowsWeight
        ON paths.endPerson = KnowsWeight.Person1Id
     WHERE NOT paths.path && ARRAY[KnowsWeight.Person2Id] -- person2Id is not in the path yet
        -- stop condition
       AND paths.person2Reached = 0
    )
SELECT
  Person1.id AS Person1Id,
  paths.endPerson AS Person2Id,
  min(paths.weight) AS totalWeight
FROM
  Person AS Person1,
  paths
WHERE paths.startPerson = Person1.id
  AND Person1.LocationCityId = :city1Id
GROUP BY
  Person1.id,
  paths.endPerson
LIMIT 5
;
