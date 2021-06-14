/* Q14. International dialog
\set country1 '\'Chile\''
\set country2 '\'Argentina\''
 */
-- TODO: maybe LATERAL joins could work for top-1 selection
WITH Person1Candidates AS (
    SELECT Person.id AS id
         , City.id AS LocationCityId
      FROM Country
      JOIN City
        ON City.PartOfCountryId = Country.id
      JOIN Person
        ON Person.LocationCityId = City.id
     WHERE Country.name = :country1
)
,  Person2Candidates AS (
    SELECT Person.id AS id
      FROM Country
      JOIN City
        ON City.PartOfCountryId = Country.id
      JOIN Person
        ON Person.LocationCityId = City.id     
     WHERE Country.name = :country2
)
,  case1 AS (
    SELECT DISTINCT
           p1.id AS Person1Id
         , p2.id AS Person2Id
         , 4 AS score
      FROM Person1Candidates p1
         , Person2Candidates p2
         , Message m -- message by p2
         , Message r -- reply by p1
     WHERE
        -- join
           m.id = r.ParentMessageId
       AND p1.id = r.CreatorPersonId
       AND p2.id = m.CreatorPersonId
)
,  case2 AS (
    SELECT DISTINCT
           p1.id AS Person1Id
         , p2.id AS Person2Id
         , 1 AS score
      FROM Person1Candidates p1
         , Person2Candidates p2
         , Message m -- message by p1
         , Message r -- reply by p2
     WHERE
        -- join
           m.id = r.ParentMessageId
       AND p2.id = r.CreatorPersonId
       AND p1.id = m.CreatorPersonId
)
,  case3 AS (
    SELECT -- no need for distinct
           p1.id AS Person1Id
         , p2.id AS Person2Id
         , 15 AS score
      FROM Person1Candidates p1
         , Person2Candidates p2
         , Person_knows_Person
     WHERE
        -- join
           p1.id = Person_knows_Person.Person1Id
       AND p2.id = Person_knows_Person.Person2Id
)
,  case4 AS (
    SELECT DISTINCT
           p1.id AS Person1Id
         , p2.id AS Person2Id
         , 10 AS score
      FROM Person1Candidates p1
         , Person2Candidates p2
         , Message m -- message by p2
         , Person_likes_Message l
     WHERE
        -- join
           p2.id = m.CreatorPersonId
       AND m.id = l.MessageId
       AND l.PersonId = p1.id
)
,  case5 AS (
    SELECT DISTINCT
           p1.id AS Person1Id
         , p2.id AS Person2Id
         , 1 AS score
      FROM Person1Candidates p1
         , Person2Candidates p2
         , Message m -- message by p1
         , Person_likes_Message l
     WHERE
        -- join
           p1.id = m.CreatorPersonId
       AND m.id = l.MessageId
       AND l.PersonId = p2.id
)
,  pair_scores AS (
    SELECT Person1Id, Person2Id, sum(score) AS score
      FROM (SELECT * FROM case1
            UNION ALL SELECT * FROM case2
            UNION ALL SELECT * FROM case3
            UNION ALL SELECT * FROM case4
            UNION ALL SELECT * FROM case5
           ) t
     GROUP BY Person1Id, Person2Id
)
,  score_ranks AS (
    SELECT s.Person1Id
         , s.Person2Id
         , City.name AS cityName
         , s.score
         , row_number() OVER (PARTITION BY City.id ORDER BY s.score DESC NULLS LAST, s.Person1Id, s.Person2Id) AS rownum
      FROM Country
           INNER JOIN City ON (Country.id = City.PartOfCountryId)
            LEFT JOIN Person1Candidates p1 ON (City.id = p1.LocationCityId)
            LEFT JOIN pair_scores s ON (p1.id = s.Person1Id)
     WHERE
        -- filter
           Country.name = :country1
)
SELECT score_ranks.Person1Id AS "person1.id"
     , score_ranks.Person2Id AS "person2.id"
     , score_ranks.cityName AS "city1.name"
     , score_ranks.score
  FROM score_ranks
 WHERE
    -- filter
       score_ranks.rownum = 1
 ORDER BY score_ranks.score DESC, score_ranks.Person1Id, score_ranks.Person2Id
 LIMIT 100
;
