/* Q18. Friend recommendation
\set tag '\'Frank_Sinatra\''
 */
SELECT k1.Person1Id AS "person1.id", k2.Person2Id AS "person2.id", count(DISTINCT k1.Person2Id) AS mutualFriendCount
FROM Person_knows_Person k1
JOIN Person_knows_Person k2
  ON k1.Person2Id = k2.Person1Id -- pattern: mutualFriend
JOIN Person_hasInterest_Tag pt1
  ON pt1.PersonId = k2.Person2Id -- pattern: person2
JOIN Person_hasInterest_Tag pt2
  ON pt2.PersonId = k1.Person1Id -- pattern: person1
JOIN Tag
  ON Tag.name = :tag
 AND pt1.TagId = Tag.id
 AND pt2.TagId = Tag.id
 -- negative edge
WHERE k1.Person1Id != k2.Person2Id
  AND NOT EXISTS (SELECT 1
         FROM Person_knows_Person k3
        WHERE k3.Person1Id = k2.Person2Id -- pattern: person2
          AND k3.Person2Id = k1.Person1Id -- pattern: person1
      )
GROUP BY k1.Person1Id, k2.Person2Id
ORDER BY mutualFriendCount DESC, k1.Person1Id ASC, k2.Person2Id ASC
LIMIT 20
;
