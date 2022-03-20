SELECT
    tagClassName AS 'tagClass:STRING',
    countryName AS 'country:STRING'
FROM
    tagClassNumMessages,
    countryNumPersons
ORDER BY md5(concat(tagClassName, countryName))
