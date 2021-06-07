/* Q11. Friend triangles
\set country '\'Belarus\''
\set startDate '\'2010-06-01T00:00:00.000+00:00\''::timestamp
 */
WITH Persons_of_country_w_friends AS (
    SELECT Person.id AS PersonId
         , Person_knows_Person.Person2Id AS FriendId
         , Person_knows_Person.creationDate AS creationDate
      FROM Person
         , City
         , Country
         , Person_knows_Person
     WHERE
        -- join
           Person.LocationCityId = City.id
       AND City.PartOfCountryId = Country.id
       AND Person.id = Person_knows_Person.Person1Id
        -- filter
       AND Country.name = :country
)
SELECT count(*)
  FROM Persons_of_country_w_friends p1
     , Persons_of_country_w_friends p2
     , Persons_of_country_w_friends p3
 WHERE
    -- join
       p1.FriendId = p2.PersonId
   AND p2.FriendId = p3.PersonId
   AND p3.FriendId = p1.PersonId
    -- filter: unique triangles only
   AND p1.PersonId < p2.PersonId
   AND p2.PersonId < p3.PersonId
    -- filter: only edges created after :startDate
   AND :startDate <= p1.creationDate
   AND :startDate <= p2.creationDate
   AND :startDate <= p3.creationDate
;
