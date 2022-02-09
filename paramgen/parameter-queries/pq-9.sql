SELECT
    creationDayNumMessages.creationDay AS 'startDate:DATE',
    creationDayNumMessages.creationDay + 8 + CAST(FLOOR(3*RANDOM()) AS INT) AS 'endDate:DATE'
FROM
    (SELECT * FROM creationDayNumMessages LIMIT 400) creationDayNumMessages
