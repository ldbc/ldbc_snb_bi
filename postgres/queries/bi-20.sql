/* Q20. Recruitment
\set company '\'Pamir_Airways\''
\set person2Id '15393162792760'
 */
WITH RECURSIVE KnowsWeight AS (
    SELECT
        Person_knows_Person.Person1Id AS Person1Id,
        Person_knows_Person.person2Id AS person2Id,
        abs(saA.classYear - saB.classYear) + 1 AS weight
    FROM Person_knows_Person
    JOIN Person_studyAt_University saA
      ON saA.PersonId = Person_knows_Person.Person1Id
    JOIN Person_studyAt_University saB
      ON saB.PersonId = Person_knows_Person.person2Id
     AND saA.UniversityId = saB.UniversityId
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
         , max(CASE WHEN Person2Id = :person2Id THEN 1 ELSE 0 END) OVER () AS person2Reached
      FROM KnowsWeight
     --WHERE Person1Id = :person1id
  UNION ALL
    SELECT paths.startPerson AS startPerson
         , KnowsWeight.Person2Id AS endPerson
         , array_append(path, Person2Id) AS path
         , KnowsWeight.weight + paths.weight AS weight
         , max(CASE WHEN KnowsWeight.Person2Id = :person2Id THEN 1 ELSE 0 END) OVER () AS person2Reached
      FROM paths
      JOIN KnowsWeight
        ON paths.endPerson = KnowsWeight.Person1Id
     WHERE NOT paths.path && ARRAY[KnowsWeight.Person2Id] -- person2Id is not in the path yet
        -- stop condition
       AND paths.person2Reached = 0
)
SELECT paths.startPerson AS Person1Id, min(paths.weight) AS totalWeight
  FROM Company
  JOIN Person_workAt_Company
    ON Person_workAt_Company.CompanyId = Company.Id
  JOIN paths
    ON paths.startPerson = Person_workAt_Company.PersonId -- person1
   AND paths.endPerson = :person2Id
 WHERE name = :company
GROUP BY paths.startPerson, paths.endPerson, paths.path
;
