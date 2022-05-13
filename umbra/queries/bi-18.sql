/* Q18. Friend recommendation
\set tag '\'Frank_Sinatra\''
 */
WITH interestedPeople as (
  SELECT DISTINCT pt.PersonId as PersonId, pp.Person2Id as FriendId
  FROM Person_hasInterest_Tag pt, Person_knows_person pp
  WHERE TagId IN (SELECT Id FROM Tag where Tag.name = :tag) and pt.PersonId = pp.Person1Id
)
SELECT p1.PersonId, p2.PersonId, count(DISTINCT p1.FriendId) AS mutualFriendCount
FROM interestedPeople p1, interestedPeople p2
WHERE p1.FriendId = p2.FriendId  -- pattern: mutualFriend
  AND p1.PersonId <> p2.PersonId
  AND NOT EXISTS (SELECT 1
        FROM Person_knows_Person pp
        WHERE pp.Person1Id = p1.PersonId -- pattern: person1
          AND pp.Person2Id = p2.PersonId -- pattern: person2
      )
GROUP BY p1.PersonId, p2.PersonId
ORDER BY mutualFriendCount DESC, p1.PersonId ASC, p2.PersonId ASC
LIMIT 20
;
