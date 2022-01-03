SELECT
    tagClassNumMessages.tagClassName AS 'tagClass:STRING',
    countryNumPersons.countryName AS 'country:STRING'
FROM
    (SELECT * FROM tagClassNumMessages LIMIT 10) tagClassNumMessages,
    (SELECT * FROM countryNumPersons LIMIT  2) countryNumPersons
