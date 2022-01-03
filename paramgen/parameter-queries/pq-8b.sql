SELECT
    tagNumMessages.tagName AS 'tag:STRING',
    creationDayNumMessages.creationDay AS 'startDate:DATE',
    creationDayNumMessages.creationDay + INTERVAL 8 DAY AS 'endDate:DATE'
FROM
    (SELECT * FROM tagNumMessages LIMIT 10) tagNumMessages,
    (SELECT * FROM creationDayNumMessages LIMIT 10) creationDayNumMessages
