/* Q14. International dialog
\set country1 '\'Chile\''
\set country2 '\'Argentina\''
 */
-- TODO: maybe LATERAL joins could work for top-1 selection
WITH PersonPairCandidates AS (
    SELECT Person1.id AS Person1Id
         , Person2.id AS Person2Id
         , City1.id AS Person1LocationCityId
      FROM Country Country1
      JOIN City City1
        ON City1.PartOfCountryId = Country1.id
      JOIN Person Person1
        ON Person1.LocationCityId = City1.id
      JOIN Person_knows_Person
        ON Person_knows_Person.Person1Id = Person1.id
      JOIN Person Person2
        ON Person2.id = Person_knows_Person.Person2Id
      JOIN City City2
        ON Person2.LocationCityId = City2.id
      JOIN Country Country2
        ON Country2.id = City2.PartOfCountryId
     WHERE Country1.name = :country1
       AND Country2.name = :country2
)
,  case1 AS (
    SELECT DISTINCT Person1Id, Person2Id, 4 AS score
      FROM PersonPairCandidates
         , Message m -- message by p2
         , Message r -- reply by p1
     WHERE
        -- join
           m.MessageId = r.ParentMessageId
       AND Person1Id = r.CreatorPersonId
       AND Person2Id = m.CreatorPersonId
)
,  case2 AS (
    SELECT DISTINCT Person1Id, Person2Id, 1 AS score
      FROM PersonPairCandidates
         , Message m -- message by p1
         , Message r -- reply by p2
     WHERE
        -- join
           m.MessageId = r.ParentMessageId
       AND Person2Id = r.CreatorPersonId
       AND Person1Id = m.CreatorPersonId
)
,  case3 AS (
    SELECT DISTINCT Person1Id, Person2Id, 10 AS score
      FROM PersonPairCandidates
         , Message m -- message by p2
         , Person_likes_Message l
     WHERE
        -- join
           Person2Id = m.CreatorPersonId
       AND m.MessageId = l.MessageId
       AND l.PersonId = Person1Id
)
,  case4 AS (
    SELECT DISTINCT Person1Id, Person2Id, 1 AS score
      FROM PersonPairCandidates
         , Message m -- message by p1
         , Person_likes_Message l
     WHERE
        -- join
           Person1Id = m.CreatorPersonId
       AND m.MessageId = l.MessageId
       AND l.PersonId = Person2Id
)
,  pair_scores AS (
    SELECT Person1Id, Person2Id, sum(score) AS score
      FROM (          SELECT * FROM case1
            UNION ALL SELECT * FROM case2
            UNION ALL SELECT * FROM case3
            UNION ALL SELECT * FROM case4
           ) t
     GROUP BY Person1Id, Person2Id
)
,  score_ranks AS (
    SELECT PersonPairCandidates.Person1Id
         , PersonPairCandidates.Person2Id
         , City.name AS cityName
         , coalesce(s.score, 0) AS score
         , row_number() OVER (PARTITION BY City.id ORDER BY s.score DESC NULLS LAST, PersonPairCandidates.Person1Id, PersonPairCandidates.Person2Id) AS rownum
      FROM Country
      JOIN City
        ON City.PartOfCountryId = Country.id
      JOIN PersonPairCandidates
        ON PersonPairCandidates.Person1LocationCityId = City.id
      LEFT JOIN pair_scores s
             ON s.Person1Id = PersonPairCandidates.Person1Id
            AND s.person2Id = PersonPairCandidates.Person2Id
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
;
