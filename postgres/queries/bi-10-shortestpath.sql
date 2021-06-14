/* Q10. Experts in social circle using shortest path semantics between startPerson and friends
\set personId 30786325588624
\set country '\'China\''
\set tagClass '\'MusicalArtist\''
\set minPathDistance 2
\set maxPathDistance 3
 */
WITH RECURSIVE friends(startPerson, hopCount, friend) AS (
    SELECT :personId AS startPersonId, 0, :personId AS friendId
  UNION
    SELECT f.startPerson
         , f.hopCount+1
         , CASE WHEN f.friend = k.Person1Id then k.Person2Id ELSE k.Person1Id END
      FROM friends f
      JOIN Person_knows_Person k
        ON k.Person1Id = f.friend -- note, that knows table have both (p1, p2) and (p2, p1)
     WHERE f.hopCount < :maxPathDistance
)
   , FriendsShortest AS (
     -- if a friend is reachable from startPerson using hopCount 2, 3 and 4, its distance from startPerson is 2
    SELECT startPerson, min(hopCount) AS hopCount, friend
      FROM friends
     GROUP BY startPerson, friend
)
   , FriendList AS (
    SELECT DISTINCT FriendsShortest.friend AS friendid
      FROM FriendsShortest
      JOIN Person tf -- the friend's person record
        ON tf.id = FriendsShortest.friend
      JOIN City
        ON City.id = tf.LocationCityId
      JOIN Country
        ON Country.id = City.PartOfCountryId
       AND Country.name = :country
     WHERE FriendsShortest.hopCount BETWEEN :minPathDistance AND :maxPathDistance
)
   , messages_of_tagclass_by_friends AS (
    SELECT DISTINCT FriendList.friendId AS friendId
         , Message.id AS MessageId
      FROM FriendList
      JOIN Message
        ON Message.CreatorPersonId = FriendList.friendId
      JOIN Message_hasTag_Tag
        ON Message_hasTag_Tag.MessageId = Message.id
      JOIN Tag
        ON Tag.id = Message_hasTag_Tag.TagId
      JOIN TagClass
        ON TagClass.id = Tag.TypeTagClassId
     WHERE TagClass.name = :tagClass
)
SELECT m.friendId AS "person.id"
     , Tag.name AS "tag.name"
     , count(*) AS messageCount
  FROM messages_of_tagclass_by_friends m
  JOIN Message_hasTag_Tag
    ON Message_hasTag_Tag.MessageId = m.MessageId
  JOIN Tag
    ON Tag.id = Message_hasTag_Tag.TagId
 GROUP BY m.friendId, Tag.name
 ORDER BY messageCount DESC, Tag.name, m.friendId
 LIMIT 100
;
