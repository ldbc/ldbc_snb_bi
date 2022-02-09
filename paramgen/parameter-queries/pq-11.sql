-- usie creationDayNumMessages to determine the startDay
SELECT
    country AS 'country:STRING',
    startDate AS 'startDate:DATE',
    startDate + INTERVAL 10 DAY AS 'endDate:DATE'
FROM
    (SELECT countryName AS country FROM countryNumPersons LIMIT 100) c, -- OFFSET 2
    (SELECT creationDay AS startDate FROM creationDayNumMessages LIMIT 100) startDate
    LIMIT 400
