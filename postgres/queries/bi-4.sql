/* Q4. Top message creators by country
\set country '\'Belarus\''
 */
WITH Top100_Popular_Forums AS (
  SELECT Forum_hasMember_Person.ForumId AS id
    FROM Forum_hasMember_Person
       , Person
       , City
       , Country
   WHERE
      -- join
         Forum_hasMember_Person.PersonId = Person.id
     AND Person.LocationCityId = City.id
     AND City.PartOfCountryId = Country.id
      -- filter
     AND Country.name = :country
   GROUP BY ForumId
   ORDER BY count(*) DESC, ForumId
   LIMIT 100
)
SELECT au.id AS "person.id"
     , au.firstName AS "person.firstName"
     , au.lastName AS "person.lastName"
     , au.creationDate
     -- a single person might be member of more than 1 of the top100 forums, so their messages should be DISTINCT counted
     , count(DISTINCT Message.id) AS messageCount
     -- TODO: count (message)-[:REPLY_OF*0]->(message)-[:CONTAINER_OF]->(forum)
  FROM Top100_Popular_Forums
       INNER JOIN Forum_hasMember_Person
               ON Top100_Popular_Forums.id = Forum_hasMember_Person.ForumId
       -- author of the message
       INNER JOIN Person au
               ON Forum_hasMember_Person.PersonId = au.id
       LEFT JOIN Message
              ON au.id = Message.CreatorPersonId
       AND Message.ContainerForumId IN (SELECT id FROM Top100_Popular_Forums)
GROUP BY au.id, au.firstName, au.lastName, au.creationDate
ORDER BY messageCount DESC, au.id
LIMIT 100
;
