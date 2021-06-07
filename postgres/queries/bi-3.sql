/* Q3. Popular topics in a country
\set tagClass '\'MusicalArtist\''
\set country '\'Burma\''
 */
SELECT Forum.id                AS "forum.id"
     , Forum.title             AS "forum.title"
     , Forum.creationDate      AS "forum.creationDate"
     , Forum.ModeratorPersonId AS "person.id"
     , count(DISTINCT Message.id) AS messageCount
     -- TODO: count (message)-[:REPLY_OF*0]->(post)-[:CONTAINER_OF]->(forum)
  FROM tagClass
     , tag
     , Message_hasTag_Tag
     , Message
     , Forum
     , Person AS ModeratorPerson -- moderator
     , City
     , Country
 WHERE
    -- join
       TagClass.id = Tag.TypeTagClassId
   AND Tag.id = Message_hasTag_Tag.TagId
   AND Message_hasTag_Tag.MessageId = Message.id
   AND Message.ContainerForumId = Forum.id
   AND Forum.ModeratorPersonId = ModeratorPerson.id
   AND ModeratorPerson.LocationCityId = City.id
   AND City.PartOfCountryId = Country.id
    -- filter
   AND TagClass.name = :tagClass
   AND Country.name = :country
 GROUP BY Forum.id, Forum.title, Forum.creationDate, Forum.ModeratorPersonId
 ORDER BY messageCount DESC, Forum.id
 LIMIT 20
;
