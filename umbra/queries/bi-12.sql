/* Q12. How many persons have a given number of messages
\set date '\'2010-07-22\''::timestamp
\set lengthThreshold '20'
\set languages '\'{"ar", "hu"}\''::varchar[]
 */
WITH person_w_posts AS (
    SELECT Person.id, count(MessageThread.MessageId) as messageCount
      FROM Person
      LEFT JOIN MessageThread
        ON Person.id = MessageThread.CreatorPersonId
       AND MessageThread.content IS NOT NULL
       AND MessageThread.length < :lengthThreshold
       AND MessageThread.creationDate > :date
       AND MessageThread.RootPostLanguage = ANY(:languages)
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
