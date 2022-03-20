SELECT
    companyName AS 'company:STRING',
    personId AS 'person2Id:ID'
FROM
    companyNumEmployees,
    (SELECT personId FROM personNumFriends ORDER BY personId LIMIT 100)
ORDER BY md5(3532569367*companyId + 211*personId)
LIMIT 400
