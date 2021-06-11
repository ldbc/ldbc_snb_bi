/* Q10. Experts in social circle using shortest path semantics between startPerson and friends
\set personId 19791209310731
\set country '\'Pakistan\''
\set tagClass '\'MusicalArtist\''
\set minPathDistance 3
\set maxPathDistance 5
 */
WITH RECURSIVE friends(startPerson, hopCount, friend) AS (
    SELECT :personId AS startPersonId, 0, :personId AS friendId
  UNION
    SELECT f.startPerson
         , f.hopCount+1
         , CASE WHEN f.friend = k.Person1Id then k.Person2Id ELSE k.Person1Id END
      FROM friends f
         , Person_knows_Person k
     WHERE
        -- join
           f.friend = k.Person1Id -- note, that knows table have both (p1, p2) and (p2, p1)
        -- filter
        -- stop condition
       AND f.hopCount < :maxPathDistance
)
   , friends_shortest AS (
     -- if a friend is reachable from startPerson using hopCount 2, 3 and 4, its distance from startPerson is 2
    SELECT startPerson, min(hopCount) AS hopCount, friend
      FROM friends
     GROUP BY startPerson, friend
)
   , friend_list AS (
    SELECT DISTINCT friends_shortest.friend AS friendid
      FROM friends_shortest
         , Person tf -- the friend's person record
         , City -- city
         , Country -- country
     WHERE
        -- join
           friends_shortest.friend = tf.id
       AND tf.LocationCityId = City.id
       AND City.PartOfCountryId = Country.id
        -- filter
       AND friends_shortest.hopCount BETWEEN :minPathDistance AND :maxPathDistance
       AND Country.name = :country
)
   , messages_of_tagclass_by_friends AS (
    SELECT DISTINCT friend_list.friendId AS friendId
         , Message.id AS MessageId
      FROM friend_list
         , Message
         , Message_hasTag_Tag
         , Tag
         , TagClass
     WHERE
        -- join
           friend_list.friendId = Message.CreatorPersonId
       AND Message.id = Message_hasTag_Tag.MessageId
       AND Message_hasTag_Tag.TagId = Tag.id
       AND Tag.TypeTagClassId = TagClass.id
        -- filter
       AND TagClass.name = :tagClass
)
SELECT m.friendId AS "person.id"
     , Tag.name AS "tag.name"
     , count(*) AS messageCount
  FROM messages_of_tagclass_by_friends m
     , Message_hasTag_Tag
     , Tag
 WHERE
    -- join
       m.MessageId = Message_hasTag_Tag.MessageId
   AND Message_hasTag_Tag.TagId = Tag.id
 GROUP BY m.friendId, Tag.name
 ORDER BY messageCount DESC, Tag.name, m.friendId
 LIMIT 100
;
