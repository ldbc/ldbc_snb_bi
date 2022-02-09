SELECT
    creationDayNumMessages.creationDay AS 'date:DATE',
    creationDayAndTagClassNumMessages.tagClassName AS 'tagClass:STRING'
FROM
    (SELECT * FROM creationDayNumMessages LIMIT 100) creationDayNumMessages,
    (SELECT * FROM creationDayAndTagClassNumMessages LIMIT 100) creationDayAndTagClassNumMessages
LIMIT 400
