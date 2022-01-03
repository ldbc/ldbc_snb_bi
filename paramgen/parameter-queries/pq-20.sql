SELECT
    companyNumEmployees.companyName AS 'company:STRING',
    personNumFriends.personId AS 'person2Id:ID'
FROM
    (SELECT * FROM companyNumEmployees LIMIT 10) companyNumEmployees,
    (SELECT * FROM personNumFriends LIMIT 10) personNumFriends
