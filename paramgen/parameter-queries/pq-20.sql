SELECT
    companyNumEmployees.companyName AS 'company:STRING',
    personNumFriends.personId AS 'person2Id:ID'
FROM
    (SELECT * FROM companyNumEmployees LIMIT 100) companyNumEmployees,
    (SELECT * FROM personNumFriends LIMIT 100) personNumFriends
LIMIT 400
