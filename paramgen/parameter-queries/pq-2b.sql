SELECT
    creationDayNumMessages.creationDay AS 'date:DATE',
    creationDayAndTagClassNumMessages.tagClassName AS 'tagClass:STRING'
FROM
    (SELECT * FROM creationDayNumMessages LIMIT 10) creationDayNumMessages,
    (SELECT * FROM creationDayAndTagClassNumMessages LIMIT 10) creationDayAndTagClassNumMessages
