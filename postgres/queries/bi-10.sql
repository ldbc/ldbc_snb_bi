/* Q10. Experts in social circle
\set personId 30786325588624
\set country '\'China\''
\set tagClass '\'MusicalArtist\''
\set minPathDistance 2
\set maxPathDistance 3

For the SF1 database size, this query completes in a reasonable time for maxPathDistance <= 4.
Above that, I also encountered the following error because of the explosion in the number of paths.
  ERROR:  could not write to tuplestore temporary file: No space left on device
 */
WITH RECURSIVE friends(startPersonId, path, friendId) AS (
    SELECT :personId AS startPersonId, ARRAY[]::record[], :personId AS friendId
  UNION ALL
    SELECT f.startPersonId
         , f.path || ROW(k.Person1id, k.Person2id)
         , CASE WHEN f.friendId = k.Person1id THEN k.Person2id ELSE k.Person1id END
      FROM friends f
      JOIN Person_knows_Person k
        ON k.Person1id = f.friendId
     WHERE true
       -- knows edge can't be traversed twice
       AND NOT ARRAY[ROW(k.Person1id, k.Person2id), ROW(k.Person2id, k.Person1id)] && f.path
        -- stop condition
       AND coalesce(array_length(f.path, 1), 0) < :maxPathDistance
)
   , friend_list AS (
    SELECT DISTINCT f.friendId AS friendId
      FROM Friends f
      JOIN Person tf -- the friend's person record
        ON tf.id = f.friendId
      JOIN City
        ON City.id = tf.LocationCityId
      JOIN Country
        ON Country.id = City.PartOfCountryId
       AND Country.name = :country
     WHERE coalesce(array_length(f.path, 1), 0) BETWEEN :minPathDistance AND :maxPathDistance
)
   , messages_of_tagclass_by_friends AS (
    SELECT DISTINCT f.friendId
         , Message.id AS messageid
      FROM friend_list f
      JOIN Message
        ON Message.CreatorPersonId = f.friendId
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
