/* Q12. How many persons have a given number of messages
\set startDate '\'2010-07-22\''::timestamp
\set lengthThreshold '20'
\set languages '\'{"ar", "hu"}\''::varchar[]
 */
WITH person_w_posts AS (
    SELECT Message.CreatorPersonId, count(Message.MessageId) as messageCount
      FROM Message
     WHERE Message.content IS NOT NULL
       AND Message.length < :lengthThreshold
       AND Message.creationDate > :startDate
       AND Message.RootPostLanguage IN :languages
     GROUP BY Message.CreatorPersonId
)
, message_count_distribution AS (
    SELECT pp.messageCount, count(*) as personCount
      FROM person_w_posts pp
     GROUP BY pp.messageCount
     ORDER BY personCount DESC, messageCount DESC
)
SELECT *
  FROM message_count_distribution
ORDER BY personCount DESC, messageCount DESC
;
