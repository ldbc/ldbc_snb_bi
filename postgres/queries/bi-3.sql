/* Q3. Popular topics in a country
\set tagClass '\'MusicalArtist\''
\set country '\'Burma\''
 */
SELECT Forum.id                AS "forum.id"
     , Forum.title             AS "forum.title"
     , Forum.creationDate      AS "forum.creationDate"
     , Forum.ModeratorPersonId AS "person.id"
     , count(DISTINCT MessageThread.MessageId) AS messageCount
  FROM tagClass
     , tag
     , Message_hasTag_Tag
     , MessageThread
     , Forum
     , Person AS ModeratorPerson -- moderator
     , City
     , Country
 WHERE
    -- join
       TagClass.id = Tag.TypeTagClassId
   AND Tag.id = Message_hasTag_Tag.TagId
   AND Message_hasTag_Tag.MessageId = MessageThread.MessageId
   AND MessageThread.ContainerForumId = Forum.id
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
