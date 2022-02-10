SELECT
    countryNumPersons.countryName AS 'country:STRING',
    creationDay AS 'endDate:DATE'
FROM
    (SELECT * FROM countryNumPersons LIMIT  10) countryNumPersons,
    (SELECT * FROM creationDayNumMessages ORDER BY creationDay DESC LIMIT 100) creationDayNumMessages
    LIMIT 400
