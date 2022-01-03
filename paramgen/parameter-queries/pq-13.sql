SELECT
    countryNumPersons.countryName AS 'country:STRING',
    creationDay AS 'endDate:DATE'
FROM
    (SELECT * FROM countryNumPersons LIMIT  2) countryNumPersons,
    (SELECT * FROM creationDayNumMessages ORDER BY creationDay DESC LIMIT 10) creationDayNumMessages
