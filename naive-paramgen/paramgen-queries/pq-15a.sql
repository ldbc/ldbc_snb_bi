SELECT
    p1.personId AS 'person1Id:ID',
    p2.personId AS 'person2Id:ID'
FROM
    (SELECT personId FROM personNumFriends ORDER BY md5(personId) ASC  LIMIT 20) p1,
    (SELECT personId FROM personNumFriends ORDER BY md5(personId) DESC LIMIT 20) p2
ORDER BY md5(131*p1.personId + 241*p2.personId), md5(p1.personId)
LIMIT 400
