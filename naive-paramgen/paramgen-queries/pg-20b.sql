SELECT
    companyNumEmployees.name AS 'company:STRING',
    personNumFriends_sample.id AS 'person2Id:ID'
FROM
    companyNumEmployees,
    (SELECT id FROM personNumFriends ORDER BY md5(id::VARCHAR) LIMIT 100) personNumFriends_sample
ORDER BY md5((3532569367::BIGINT*companyNumEmployees.id + 211::BIGINT*personNumFriends_sample.id)::VARCHAR)
LIMIT 400
