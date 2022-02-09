SELECT
    tagClassNumMessages.tagClassName AS 'tagClass:STRING',
    countryNumPersons.countryName AS 'country:STRING'
FROM
    (SELECT * FROM tagClassNumMessages LIMIT 100) tagClassNumMessages,
    (SELECT * FROM countryNumPersons LIMIT  10) countryNumPersons
 LIMIT 400
