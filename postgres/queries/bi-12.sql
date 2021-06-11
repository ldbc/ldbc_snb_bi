/* Q12. How many persons have a given number of messages
\set date '\'2010-07-22T00:00:00.000+00:00\''::timestamp
\set lengthThreshold '20'
\set languages '\'{"ar", "hu"}\''::varchar[]
 */
WITH RECURSIVE message_all(id, language, CreatorPersonId, content_isempty, length, creationDay) AS (
    SELECT id, language, CreatorPersonId
         , content IS NULL AS content_isempty
         , length
         , creationDate --date_trunc('day', creationDate) AS creationDay
      FROM Post
  UNION ALL
    SELECT Comment.id AS id, message_all.language, Comment.CreatorPersonId AS CreatorPersonId
         , Comment.content IS NULL AS content_isempty
         , Comment.length
         , Comment.creationDate --date_trunc('day', creationDate) AS creationDay
      FROM Comment, message_all
     WHERE coalesce(Comment.ParentPostId, Comment.ParentCommentId) = message_all.id
)
, person_w_posts AS (
    SELECT Person.id, count(message_all.id) as messageCount
      FROM Person
      LEFT JOIN message_all
        ON Person.id = message_all.CreatorPersonId
       AND NOT message_all.content_isempty
       AND length < :lengthThreshold
       AND creationDay > :date
       AND language = ANY(:languages)
     GROUP BY Person.id
)
, message_count_distribution AS (
    SELECT pp.messageCount, count(*) as personCount
      FROM person_w_posts pp
     GROUP BY pp.messageCount
     ORDER BY personCount DESC, messageCount DESC
)
SELECT *
  FROM message_count_distribution
;
