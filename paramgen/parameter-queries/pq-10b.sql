-- potential minPathDistance and maxPathDistance values: 1..2, 1..3, 2..2, 2..3
SELECT
    personNumFriends.personId AS 'personId:ID',
    countryNumPersons.countryName AS 'country:STRING',
    tagClassNumMessages.tagClassName AS 'tagClass:STRING',
    1+CAST(FLOOR(2*RANDOM()) AS INT) AS 'minPathDistance:INT',
    2+CAST(FLOOR(2*RANDOM()) AS INT) AS 'maxPathDistance:INT'
FROM
    (SELECT * FROM personNumFriends LIMIT 10) personNumFriends,
    (SELECT * FROM countryNumPersons LIMIT 10) countryNumPersons, -- OFFSET 2
    (SELECT * FROM tagClassNumMessages LIMIT 10) tagClassNumMessages
