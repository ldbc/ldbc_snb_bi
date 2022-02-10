SELECT
    tagNumMessages.tagName AS 'tag:STRING',
    creationDayNumMessages.creationDay AS 'startDate:DATE',
    creationDayNumMessages.creationDay + INTERVAL 8 DAY AS 'endDate:DATE'
FROM
    (SELECT * FROM tagNumMessages LIMIT 100) tagNumMessages,
    (SELECT * FROM creationDayNumMessages LIMIT 100) creationDayNumMessages
LIMIT 400
