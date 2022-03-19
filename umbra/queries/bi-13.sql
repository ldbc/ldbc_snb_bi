/* Q13. Zombies in a country
\set country '\'France\''
\set endDate '\'2013-01-01\''::timestamp
 */
WITH Zombies AS (
    SELECT Person.id AS zombieid
      FROM Country
      JOIN City
        ON City.PartOfCountryId = Country.id
      JOIN Person
        ON Person.LocationCityId = City.id
      LEFT JOIN Message
         ON Person.id = Message.CreatorPersonId
        AND Message.creationDate BETWEEN Person.creationDate AND :endDate -- the lower bound is an optmization to prune messages
     WHERE Country.name = :country
       AND Person.creationDate < :endDate
     GROUP BY Person.id, Person.creationDate
        -- average of [0, 1) messages per month is equivalent with having less messages than the month span between person creationDate and parameter :endDate
    HAVING count(Message.MessageId) < 12*extract(YEAR FROM :endDate) + extract(MONTH FROM :endDate)
                            - (12*extract(YEAR FROM Person.creationDate) + extract(MONTH FROM Person.creationDate))
                            + 1
)
SELECT Z.zombieid AS "zombie.id"
     , count(zl.zombieid) AS zombieLikeCount
     , count(Person_likes_Message.PersonId) AS totalLikeCount
     , CASE WHEN count(Person_likes_Message.PersonId) = 0 THEN 0 ELSE count(zl.zombieid)::float/count(Person_likes_Message.PersonId) END AS zombieScore
  FROM Message
       LEFT  JOIN Person_likes_Message ON (Message.MessageId = Person_likes_Message.MessageId)
       INNER JOIN Person ON (Person_likes_Message.PersonId = Person.id AND Person.creationDate < :endDate)
       LEFT  JOIN Zombies ZL ON (Person.id = ZL.zombieid) -- see if the like was given by a zombie
       RIGHT JOIN Zombies Z ON (Z.zombieid = Message.CreatorPersonId)
 GROUP BY Z.zombieid
 ORDER BY zombieScore DESC, Z.zombieid
 LIMIT 100
;
