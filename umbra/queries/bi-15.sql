/* Q15. Trusted connection paths through forums created in a given timeframe
\set person1Id 21990232564808
\set person2Id 26388279076936
\set startDate '\'2010-11-01\''::timestamp
\set endDate '\'2010-12-01\''::timestamp
 */

WITH RECURSIVE
   Paths(dst, path) AS (
      SELECT Person2id, ARRAY[Person1id, Person2id] FROM Person_knows_Person WHERE Person1id = :person1Id
      UNION ALL
      SELECT t.Person2id, array_append(path, t.Person2id)
      FROM (SELECT * FROM Paths WHERE NOT EXISTS (SELECT * FROM Paths s2 WHERE s2.dst = :person2Id)) s, Person_knows_Person t
      WHERE s.dst = t.Person1id
   ),
   SelectedPaths(dst, path) AS (
      SELECT dst, path
      FROM Paths
      WHERE dst = :person2Id
   ),
   Iterator(i) AS (
      SELECT i FROM (SELECT array_length(path, 1) v FROM SelectedPaths LIMIT 1) l(v), generate_series(1, l.v) t(i)
   ),
   MyForums AS (
      SELECT * FROM Forum WHERE Forum.creationDate BETWEEN :startDate AND :endDate
   ),
   PathWeights(dst, path, Score) AS (
      SELECT p.dst, p.path, SUM(Score)
      FROM SelectedPaths p, Iterator it, (
         SELECT (case when msg.ParentMessageId IS NULL then 1.0 else 0.5 end) AS Score
         FROM MessageThread msg, Message reply
         WHERE reply.ParentMessageId = msg.MessageId AND msg.CreatorPersonId = p.path[i] AND reply.CreatorPersonId = p.path[i + 1]
           AND EXISTS (SELECT * FROM MyForums WHERE msg.ContainerForumId = MyForums.Id)
         UNION ALL
         SELECT (case when msg.ParentMessageId IS NULL then 1.0 else 0.5 end) AS Score
         FROM MessageThread msg, Message reply
         WHERE reply.ParentMessageId = msg.MessageId AND msg.CreatorPersonId = p.path[i + 1] AND reply.CreatorPersonId = p.path[i]
          AND EXISTS (SELECT * FROM MyForums WHERE msg.ContainerForumId = MyForums.Id)
      ) t
      GROUP BY p.dst, p.path
   )
SELECT path, Score as Weights
FROM PathWeights
ORDER BY Weights DESC, path;
