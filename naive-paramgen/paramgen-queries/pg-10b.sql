SELECT
    personId AS 'personId:ID',
    countryName AS 'country:STRING',
    tagClassName AS 'tagClass:STRING',
    3 AS 'minPathDistance:INT',
    4 AS 'maxPathDistance:INT'
FROM
    countryNumPersons,
    (SELECT * FROM personNumFriends ORDER BY personId LIMIT 100) personNumFriends,
    tagClassNumTags
ORDER BY md5(concat(personId, countryName, tagClassName))
LIMIT 400
