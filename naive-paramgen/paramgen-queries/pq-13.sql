SELECT
    countryName AS 'country:STRING',
    creationDay AS 'endDate:DATE'
FROM
    countryNumPersons,
    creationDayNumMessages
ORDER BY md5(concat(countryName, creationDay))
LIMIT 400
